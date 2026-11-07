import base64
import json
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit


class Handler(BaseHTTPRequestHandler):
    events = []
    options = {}
    token_number = 0
    jwt_token = ""
    jwt_rejected = False

    def respond(self, value, status=200):
        body = json.dumps(value).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def record(self):
        type(self).events.append(
            {
                "path": urlsplit(self.path).path,
                "authorization": self.headers.get("Authorization", ""),
                "token_request_header": self.headers.get("X-Token-Request", ""),
                "query": urlsplit(self.path).query,
            }
        )

    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        if self.path == "/reset":
            type(self).events = []
            type(self).options = json.loads(body)
            type(self).token_number = 0
            type(self).jwt_token = ""
            type(self).jwt_rejected = False
            self.respond({})
        else:
            self.respond({}, 404)

    def do_GET(self):
        path = urlsplit(self.path).path
        if path == "/":
            body = b"OK"
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if path == "/events":
            self.respond(type(self).events)
            return
        self.record()
        if path == "/token":
            if self.headers.get("X-Token-Request") != "test":
                self.respond({}, 400)
                return
            type(self).token_number += 1
            payload = {
                "exp": int(time.time()) + type(self).options.get("jwt_lifetime", 3600),
                "jti": str(type(self).token_number),
            }
            encoded = (
                base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip("=")
            )
            token = type(self).options.get(
                "jwt_body", f"eyJhbGciOiJSUzI1NiJ9.{encoded}.signature"
            )
            type(self).jwt_token = token
            body = token.encode()
            if "jwt_json_field" in type(self).options:
                value = token
                for field in reversed(type(self).options["jwt_json_field"].split(".")):
                    value = {field: value}
                body = json.dumps(value).encode()
            if "jwt_response_body" in type(self).options:
                body = type(self).options["jwt_response_body"].encode()
            self.send_response(type(self).options.get("jwt_status", 200))
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Location", "/unexpected-redirect")
            self.end_headers()
            self.wfile.write(body)
            return
        if (
            path == "/v1/namespaces"
            and type(self).options.get("reject_jwt_once")
            and not type(self).jwt_rejected
        ):
            type(self).jwt_rejected = True
            self.respond({}, type(self).options["reject_jwt_once"])
            return
        expected = f"Bearer {type(self).jwt_token}"
        if self.headers.get("Authorization", "") != expected:
            self.respond({}, 403)
        elif path == "/v1/config":
            self.respond({"defaults": {}, "overrides": {}})
        elif path == "/v1/namespaces":
            self.respond({"namespaces": []})
        else:
            self.respond({}, 404)


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", int(sys.argv[1])), Handler).serve_forever()
