"""
A minimal RESP server that answers MGET with a reply of the test's choosing.

`StorageRedis` pairs the elements of an MGET reply with the requested keys by position, so both
the element count and the position of a nil decide what the engine does. Usage:

    fake_redis.py <port> <delta> [<scan-keys> <nil-keys>]

Every MGET is answered with `max(0, len(keys) + delta)` elements: the requested keys echoed back
as their own values, followed by RESP nils. `<scan-keys>` and `<nil-keys>` are comma-separated
key names; when they are given, SCAN answers with those keys and MGET answers nil for the names
in `<nil-keys>`, so a full scan meets a fixed valid/nil sequence rather than Redis's own key
order. Anything else is answered `+OK`.
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


def serialize_string(name):
    """A String shorter than 128 bytes as ClickHouse serializes it: one length byte, then bytes.

    The engine deserializes the keys SCAN reports and the values MGET returns, so both have to
    arrive in that form. See serialize_binary_for_string in test.py.
    """
    return bytes([len(name)]) + name.encode()


def mget_reply(keys, delta, nil_keys):
    count = max(0, len(keys) + delta)
    out = [b"*%d\r\n" % count]
    for i in range(count):
        if i < len(keys) and keys[i] not in nil_keys:
            out.append(b"$%d\r\n%s\r\n" % (len(keys[i]), keys[i]))
        else:
            out.append(b"$-1\r\n")
    return b"".join(out)


def scan_reply(scan_keys):
    """Cursor 0 with every key, so the engine reads the whole keyspace in one batch."""
    out = [b"*2\r\n$1\r\n0\r\n", b"*%d\r\n" % len(scan_keys)]
    for key in scan_keys:
        out.append(b"$%d\r\n%s\r\n" % (len(key), key))
    return b"".join(out)


def handle(conn, delta, scan_keys, nil_keys):
    try:
        with conn.makefile("rb") as stream:
            while True:
                args = read_command(stream)
                if not args:
                    break
                command = args[0].upper()
                if command == b"MGET":
                    conn.sendall(mget_reply(args[1:], delta, nil_keys))
                elif command == b"SCAN" and scan_keys:
                    conn.sendall(scan_reply(scan_keys))
                else:
                    conn.sendall(b"+OK\r\n")
    except (EOFError, OSError, ValueError):
        pass
    finally:
        conn.close()


def parse_names(argument):
    return [serialize_string(name) for name in argument.split(",") if name]


def main():
    port = int(sys.argv[1])
    delta = int(sys.argv[2])
    scan_keys = parse_names(sys.argv[3]) if len(sys.argv) > 3 else []
    nil_keys = parse_names(sys.argv[4]) if len(sys.argv) > 4 else []

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", port))
    # The engine holds a connection pool and a direct join reads on several pipeline threads, so
    # connections have to be served concurrently.
    server.listen(64)
    print(f"listening on {port}, MGET reply length = requested + {delta}", flush=True)

    while True:
        conn, _ = server.accept()
        threading.Thread(
            target=handle, args=(conn, delta, scan_keys, nil_keys), daemon=True
        ).start()


if __name__ == "__main__":
    main()
