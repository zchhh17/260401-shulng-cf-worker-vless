import { connect } from 'cloudflare:sockets';

const textDecoder = new TextDecoder();

export default {
	async fetch(request) {
		const upgradeHeader = request.headers.get('Upgrade');
		if (upgradeHeader !== 'websocket') {
			return new Response('Not found', { status: 404 });
		}
		const url = new URL(request.url);
		const proxyIP = url.searchParams.get('ip') || '';
		return await upgradeWebSocket(proxyIP);
	},
};

async function upgradeWebSocket(proxyIP) {
	const [client, server] = Object.values(new WebSocketPair());
	server.accept();
	server.binaryType = 'arraybuffer';
	server.send(new Uint8Array([0, 0]));
	transferData(server, proxyIP);
	return new Response(null, { status: 101, webSocket: client });
}

async function transferData(server, proxyIP) {
	let tcpConn;
	let writer;
	let headerParsed = false;

	const readable = new ReadableStream({
		start(controller) {
			server.addEventListener('message', (event) => {
				controller.enqueue(event.data);
			});
		},
	});

	const writable = new WritableStream({
		async write(chunk) {
			if (headerParsed) {
				await writer.write(chunk);
			} else {
				const result = parseHeader(chunk);
				if (!result) return;
				headerParsed = true;
				await establishConnection(result, proxyIP);
			}
		},
	});

	await readable.pipeTo(writable);

	async function establishConnection({ port, address, initialData }, proxyIP) {
		try {
			tcpConn = connect({ hostname: address, port });
			await tcpConn.opened;
		} catch {
			const [proxyHost, proxyPort = port] = proxyIP.split(':');
			tcpConn = connect({ hostname: proxyHost, port: proxyPort });
			await tcpConn.opened;
		}

		writer = tcpConn.writable.getWriter();

		if (initialData?.byteLength > 0) {
			await writer.write(initialData);
		}

		await tcpConn.readable.pipeTo(
			new WritableStream({
				write(chunk) {
					server.send(chunk);
				},
			}),
		);
	}
}

function parseHeader(data) {
	const view = new Uint8Array(data);
	const addrType = view[17];
	let offset = 18 + addrType + 1;

	const port = new DataView(data.slice(offset, offset + 2)).getUint16(0);
	offset += 2;

	const type = view[offset++];
	let address = '';
	let addrLen = 0;

	switch (type) {
		case 1: {
			addrLen = 4;
			address = `${view[offset]}.${view[offset + 1]}.${view[offset + 2]}.${view[offset + 3]}`;
			break;
		}
		case 2: {
			addrLen = view[offset++];
			address = textDecoder.decode(data.slice(offset, offset + addrLen));
			break;
		}
		case 3: {
			addrLen = 16;
			const parts = [];
			for (let i = 0; i < 8; i++) {
				parts.push(new DataView(data.slice(offset + i * 2, offset + i * 2 + 2)).getUint16(0).toString(16));
			}
			address = parts.join(':');
			break;
		}
		default:
			return null;
	}

	return {
		port,
		address,
		initialData: data.slice(offset + addrLen),
	};
}
