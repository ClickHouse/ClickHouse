"""A minimal HTTP forward proxy that REQUIRES Basic authentication.

Used to verify that ClickHouse sends credentials taken from the userinfo part of
`http_proxy`. Requests without a valid `Proxy-Authorization` header are answered
with 407, so a query only succeeds if the credentials really were transmitted.
"""

import base64
import socket
import threading

LISTEN_PORT = 8081
UPSTREAM = ("minio1", 9001)

USERNAME = "user"
# The environment variable percent-encodes this as p%40ssword.
PASSWORD = "p@ssword"
EXPECTED = "Basic " + base64.b64encode(f"{USERNAME}:{PASSWORD}".encode()).decode()

DENIED = (
    b"HTTP/1.1 407 Proxy Authentication Required\r\n"
    b'Proxy-Authenticate: Basic realm="proxy"\r\n'
    b"Content-Length: 0\r\n"
    b"Connection: close\r\n\r\n"
)


def read_headers(sock):
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = sock.recv(65536)
        if not chunk:
            return None
        data += chunk
    return data


def to_origin_form(request_line):
    # "GET http://minio1:9001/root/data/x HTTP/1.1" -> "GET /root/data/x HTTP/1.1"
    parts = request_line.split(" ")
    if len(parts) < 3:
        return request_line
    method, target, version = parts[0], parts[1], parts[2]
    for scheme in ("http://", "https://"):
        if target.startswith(scheme):
            rest = target[len(scheme) :]
            slash = rest.find("/")
            target = rest[slash:] if slash != -1 else "/"
            break
    return " ".join([method, target, version])


def pipe(src, dst):
    try:
        while True:
            data = src.recv(65536)
            if not data:
                break
            dst.sendall(data)
    except OSError:
        pass
    finally:
        for s in (src, dst):
            try:
                s.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass


def handle(client):
    upstream = None
    try:
        raw = read_headers(client)
        if raw is None:
            return

        head, _, rest = raw.partition(b"\r\n\r\n")
        lines = head.decode("utf-8", "replace").split("\r\n")
        request_line = lines[0]

        authorization = None
        for line in lines[1:]:
            if line.lower().startswith("proxy-authorization:"):
                authorization = line.split(":", 1)[1].strip()

        if authorization != EXPECTED:
            print(
                f"DENIED {request_line} proxy_authorization={authorization}", flush=True
            )
            client.sendall(DENIED)
            return

        print(f"ALLOWED {request_line}", flush=True)

        forwarded = [to_origin_form(request_line)]
        for line in lines[1:]:
            lowered = line.lower()
            if lowered.startswith("proxy-authorization:") or lowered.startswith(
                "proxy-connection:"
            ):
                continue
            if lowered.startswith("connection:"):
                continue
            forwarded.append(line)
        forwarded.append("Connection: close")

        upstream = socket.create_connection(UPSTREAM, timeout=30)
        upstream.sendall(("\r\n".join(forwarded) + "\r\n\r\n").encode())
        if rest:
            upstream.sendall(rest)

        back = threading.Thread(target=pipe, args=(upstream, client), daemon=True)
        back.start()
        pipe(client, upstream)
        back.join(timeout=30)
    except OSError as e:
        print(f"ERROR {e}", flush=True)
    finally:
        for s in (client, upstream):
            if s is not None:
                try:
                    s.close()
                except OSError:
                    pass


def main():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", LISTEN_PORT))
    server.listen(64)
    print(f"auth proxy listening on {LISTEN_PORT}", flush=True)
    while True:
        client, _ = server.accept()
        threading.Thread(target=handle, args=(client,), daemon=True).start()


main()
