#!/usr/bin/env python3
# Forwards MySQL connections, holding every COM_PING for a minute; writes its pid once listening.
import asyncio
import os
import sys

LISTEN_PORT, UPSTREAM_HOST, UPSTREAM_PORT, PID_FILE = (
    int(sys.argv[1]),
    sys.argv[2],
    int(sys.argv[3]),
    sys.argv[4],
)
COM_PING = 0x0E


async def client_to_server(reader, writer):
    buffer = b""
    while data := await reader.read(65536):
        buffer += data
        while len(buffer) >= 4:
            size = 4 + int.from_bytes(buffer[:3], "little")
            if len(buffer) < size:
                break
            packet, buffer = buffer[:size], buffer[size:]
            # Sequence id 0 starts a command.
            if packet[3] == 0 and size > 4 and packet[4] == COM_PING:
                await asyncio.sleep(60)
            writer.write(packet)
            await writer.drain()
    writer.close()


async def server_to_client(reader, writer):
    greeting = await reader.readexactly(4)
    greeting = bytearray(
        greeting + await reader.readexactly(int.from_bytes(greeting[:3], "little"))
    )
    # Clear CLIENT_SSL (0x0800) in the server capabilities, so the session stays in plain text.
    # Layout: header, protocol version, server version, connection id, auth data, filler, flags.
    capabilities = greeting.index(0, 5) + 1 + 4 + 8 + 1
    greeting[capabilities + 1] &= ~0x08
    writer.write(greeting)
    while data := await reader.read(65536):
        writer.write(data)
        await writer.drain()
    writer.close()


async def handle(client_reader, client_writer):
    server_reader, server_writer = await asyncio.open_connection(
        UPSTREAM_HOST, UPSTREAM_PORT
    )
    await asyncio.gather(
        client_to_server(client_reader, server_writer),
        server_to_client(server_reader, client_writer),
        return_exceptions=True,
    )


async def main():
    server = await asyncio.start_server(handle, "127.0.0.1", LISTEN_PORT)
    with open(PID_FILE, "w") as f:
        f.write(str(os.getpid()))
    await server.serve_forever()


asyncio.run(main())
