import base64
import http.server
import json

GOOD_PASSWORD = "good_password"
USER_RESPONSES = {
    "test_user_1": {"settings": {"auth_user": "'test_user'", "auth_num": "UInt64_15"}},
    "test_user_2": {},
    "test_user_3": "",
    "test_user_4": "not json string",
}


class RequestHandler(http.server.BaseHTTPRequestHandler):
    def decode_basic(self, data):
        decoded_data = base64.b64decode(data).decode("utf-8")
        return decoded_data.split(":", 1)

    def do_AUTHHEAD(self):
        self.send_response(http.HTTPStatus.UNAUTHORIZED)
        self.send_header("WWW-Authenticate", 'Basic realm="Test"')
        self.send_header("Content-type", "text/html")
        self.end_headers()

    def do_ACCESS_GRANTED(self, user: str) -> None:
        self.send_response(http.HTTPStatus.OK)
        body = ""

        response = USER_RESPONSES.get(user)

        if isinstance(response, dict):
            body = json.dumps(response)
        else:
            body = response or ""

        body_raw = body.encode("utf-8")
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(body_raw))
        self.end_headers()
        self.wfile.write(body_raw)

    def do_GET(self):
        if self.path == "/health":
            self.send_response(http.HTTPStatus.OK)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK")

        elif self.path == "/basic_auth":
            auth_header = self.headers.get("Authorization")

            if auth_header is None:
                self.do_AUTHHEAD()
                return

            auth_scheme, data = auth_header.split(" ", 1)

            if auth_scheme != "Basic":
                print(auth_scheme)
                self.do_AUTHHEAD()
                return

            user_name, password = self.decode_basic(data)
            if password == GOOD_PASSWORD:
                self.do_ACCESS_GRANTED(user_name)
            else:
                self.do_AUTHHEAD()


if __name__ == "__main__":
    httpd = http.server.HTTPServer(
        (
            "0.0.0.0",
            8000,
        ),
        RequestHandler,
    )
    try:
        httpd.serve_forever()
    finally:
        httpd.server_close()
