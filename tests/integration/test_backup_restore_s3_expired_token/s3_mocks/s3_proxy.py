import http.client
import json
import re
import sys
import threading
import time
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

state_lock = threading.Lock()
armed = None
events = []


def operation(method, path, headers):
    query = urllib.parse.parse_qs(urllib.parse.urlsplit(path).query)
    if method == "PUT" and "partNumber" in query and "x-amz-copy-source" in headers:
        return "UploadPartCopy"
    if method == "PUT" and "partNumber" in query:
        return "UploadPart"
    if method == "GET":
        return "GetObject"
    return None


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_args):
        pass

    def reply(self, status, body=b"", headers=None):
        self.send_response(status)
        for name, value in (headers or {}).items():
            if name.lower() not in (
                "content-length",
                "transfer-encoding",
                "connection",
            ):
                self.send_header(name, value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def read_body(self):
        if self.headers.get("Transfer-Encoding", "").lower() != "chunked":
            return self.rfile.read(int(self.headers.get("Content-Length", 0)))

        chunks = []
        while True:
            line = self.rfile.readline()
            chunks.append(line)
            size = int(line.split(b";", 1)[0], 16)
            if size == 0:
                while True:
                    trailer = self.rfile.readline()
                    chunks.append(trailer)
                    if trailer in (b"\r\n", b"\n", b""):
                        return b"".join(chunks)
            chunks.append(self.rfile.read(size + 2))

    def handle_control(self):
        global armed
        if self.path == "/control/events":
            self.reply(
                200, json.dumps(events).encode(), {"Content-Type": "application/json"}
            )
            return
        if self.path.startswith("/control/arm?"):
            params = urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query)
            with state_lock:
                events.clear()
                armed = {
                    "operation": params["operation"][0],
                    "prefix": params["prefix"][0],
                    "remaining": int(params["count"][0]),
                    "rotate": params.get("rotate", ["0"])[0] == "1",
                    "delay_ms": int(params.get("delay_ms", ["0"])[0]),
                }
            self.reply(200, b"OK")
            return
        self.reply(404)

    def proxy(self):
        global armed
        if self.path == "/":
            self.reply(200, b"OK")
            return
        if self.path.startswith("/control/"):
            self.handle_control()
            return

        body = self.read_body()
        current_operation = operation(self.command, self.path, self.headers)
        match = re.search(r"Credential=([^/]+)", self.headers.get("Authorization", ""))
        access_key = match.group(1) if match else ""

        inject = False
        rotate = False
        delay_ms = 0
        with state_lock:
            if (
                armed
                and current_operation == armed["operation"]
                and self.path.startswith(armed["prefix"])
            ):
                inject = armed["remaining"] > 0
                if inject:
                    armed["remaining"] -= 1
                    rotate = armed["rotate"]
                    armed["rotate"] = False
                    delay_ms = armed["delay_ms"]
                events.append(
                    {
                        "operation": current_operation,
                        "path": self.path,
                        "access_key": access_key,
                        "body_size": len(body),
                        "injected": inject,
                    }
                )

        if inject:
            if delay_ms:
                time.sleep(delay_ms / 1000)
            if rotate:
                urllib.request.urlopen(
                    urllib.request.Request(
                        "http://sts.amazonaws.com:80/toggle", method="POST"
                    ),
                    timeout=5,
                ).close()
            self.reply(
                400,
                b"<Error><Code>ExpiredToken</Code><Message>The provided token has expired.</Message></Error>",
                {"Content-Type": "application/xml", "x-amz-request-id": "injected"},
            )
            return

        connection = http.client.HTTPConnection("minio1", 9001, timeout=30)
        connection.putrequest(
            self.command, self.path, skip_host=True, skip_accept_encoding=True
        )
        for name, value in self.headers.items():
            if name.lower() != "connection":
                connection.putheader(name, value)
        connection.endheaders(body)
        upstream = connection.getresponse()
        result = upstream.read()
        headers = dict(upstream.getheaders())
        if self.command == "HEAD":
            self.send_response(upstream.status)
            for name, value in headers.items():
                if name.lower() not in ("transfer-encoding", "connection"):
                    self.send_header(name, value)
            self.end_headers()
        else:
            self.reply(upstream.status, result, headers)
        connection.close()

    do_GET = proxy
    do_HEAD = proxy
    do_PUT = proxy
    do_POST = proxy
    do_DELETE = proxy


ThreadingHTTPServer(("0.0.0.0", int(sys.argv[1])), Handler).serve_forever()
