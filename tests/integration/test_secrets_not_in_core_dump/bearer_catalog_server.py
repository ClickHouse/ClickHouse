import base64
import json
import sys
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlsplit


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        path = urlsplit(self.path).path
        if path == "/":
            body = b"OK"
        elif path == "/token":
            payload = base64.urlsafe_b64encode(
                json.dumps({"exp": int(time.time()) + 3600}).encode()
            ).rstrip(b"=")
            body = b"eyJhbGciOiJSUzI1NiJ9." + payload + b".signature"
        elif path == "/v1/config":
            body = b'{"defaults": {}, "overrides": {}}'
        elif path == "/v1/namespaces":
            body = b'{"namespaces": []}'
        else:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    HTTPServer(("0.0.0.0", int(sys.argv[1])), Handler).serve_forever()
