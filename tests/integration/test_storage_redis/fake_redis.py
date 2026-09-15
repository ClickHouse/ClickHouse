"""
A minimal RESP server that answers MGET with a deliberately wrong number of elements.

`StorageRedis` pairs the elements of an MGET reply with the requested keys by position, so a
reply whose element count differs from the request has no correct interpretation. Given
`<port> <delta>`, every MGET here is answered with `max(0, len(keys) + delta)` elements: the
requested keys echoed back as their own values, followed by RESP nils. Anything else is
answered `+OK`.
"""

import socket
import sys
import threading


def read_exactly(stream, size):
    data = stream.read(size)
    if len(data) != size:
        raise EOFError
    return data


def read_line(stream):
    line = stream.readline()
    if not line:
        raise EOFError
    return line.rstrip(b"\r\n")


def read_command(stream):
    """Read one RESP array.

    Argument payloads are read by declared length, never up to a newline: the keys on the wire
    are ClickHouse's serializeBinary output and may contain \\r, \\n or non-UTF-8 bytes.
    """
    header = read_line(stream)
    if not header.startswith(b"*"):
        return None
    args = []
    for _ in range(int(header[1:])):
        arg_header = read_line(stream)
        if not arg_header.startswith(b"$"):
            raise EOFError
        args.append(read_exactly(stream, int(arg_header[1:])))
        read_exactly(stream, 2)  # trailing CRLF
    return args


def mget_reply(keys, delta):
    count = max(0, len(keys) + delta)
    out = [b"*%d\r\n" % count]
    for i in range(count):
        if i < len(keys):
            out.append(b"$%d\r\n%s\r\n" % (len(keys[i]), keys[i]))
        else:
            out.append(b"$-1\r\n")
    return b"".join(out)


def handle(conn, delta):
    try:
        with conn.makefile("rb") as stream:
            while True:
                args = read_command(stream)
                if not args:
                    break
                if args[0].upper() == b"MGET":
                    conn.sendall(mget_reply(args[1:], delta))
                else:
                    conn.sendall(b"+OK\r\n")
    except (EOFError, OSError, ValueError):
        pass
    finally:
        conn.close()


def main():
    port = int(sys.argv[1])
    delta = int(sys.argv[2])

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", port))
    # The engine holds a connection pool and a direct join reads on several pipeline threads, so
    # connections have to be served concurrently.
    server.listen(64)
    print(f"listening on {port}, MGET reply length = requested + {delta}", flush=True)

    while True:
        conn, _ = server.accept()
        threading.Thread(target=handle, args=(conn, delta), daemon=True).start()


if __name__ == "__main__":
    main()
