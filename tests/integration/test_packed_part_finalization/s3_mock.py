import http.server
import json
import sys
import threading
from urllib.parse import parse_qs, urlsplit


condition = threading.Condition()
released = threading.Event()
released.set()
keys = {}
fail = False


class Handler(http.server.BaseHTTPRequestHandler):
    def reply(self, status, body):
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def redirect(self):
        remaining = int(self.headers.get("Content-Length", 0))
        while remaining:
            data = self.rfile.read(min(remaining, 65536))
            if not data:
                return
            remaining -= len(data)
        self.send_response(307)
        self.send_header("Location", "http://minio1:9001" + self.path)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        global fail
        path = urlsplit(self.path)
        params = parse_qs(path.query)
        if path.path == "/":
            return self.reply(200, b"OK")
        if path.path == "/gate/arm":
            with condition:
                keys.clear()
                fail = params.get("fail") == ["1"]
                released.clear()
            return self.reply(200, b"OK")
        if path.path == "/gate/wait":
            count = int(params["count"][0])
            with condition:
                condition.wait_for(lambda: len(keys) >= count, timeout=15)
                body = json.dumps(sorted(keys)).encode()
            return self.reply(200, body)
        if path.path == "/gate/multipart":
            with condition:
                body = json.dumps(sorted(set(keys.values()))).encode()
            return self.reply(200, body)
        if path.path == "/gate/release":
            released.set()
            return self.reply(200, b"OK")
        return self.redirect()

    def do_PUT(self):
        path = urlsplit(self.path)
        if path.path.endswith("/data.packed"):
            with condition:
                keys[path.path] = "partNumber" in parse_qs(path.query)
                condition.notify_all()
            if not released.wait(timeout=60):
                return self.reply(500, b"Packed upload barrier timed out")
            if fail:
                return self.reply(
                    403,
                    b"<Error><Code>AccessDenied</Code><Message>Injected packed upload failure</Message></Error>",
                )
        return self.redirect()

    do_HEAD = redirect
    do_POST = redirect
    do_DELETE = redirect


if __name__ == "__main__":
    http.server.ThreadingHTTPServer(("0.0.0.0", int(sys.argv[1])), Handler).serve_forever()
