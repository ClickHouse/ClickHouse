import base64
import http.server
import json
import threading
from typing import List
from urllib.parse import parse_qs, urlparse

GOOD_PASSWORD = "good_password"
USER_RESPONSES = {
    "test_user_1": {"settings": {"auth_user": "'test_user'", "auth_num": "UInt64_15"}},
    "test_user_2": {},
    "test_user_3": "",
    "test_user_4": "not json string",
}

# Behaviour of the `/directory` endpoint (used by the `http` user directory tests), by user name.
# `None` roles/settings mean "empty body". `tenant_user` depends on the `X-Tenant` header.
DIRECTORY_USERS = {
    "alice": {"settings": {"max_threads": "3"}, "roles": ["r_reader"]},
    "plain": {},
    "cached_user": {"roles": []},
    "dist_user": {"roles": ["r_reader"]},
    "bad_role": {"roles": ["r_admin"]},
    "ghost_role": {"roles": ["tenant_missing"]},
    "broken": 500,
}
TENANT_ROLES = {"a": ["r_a"], "b": ["r_b"]}
DIRECTORY_COUNTS = {}
DIRECTORY_LOCK = threading.Lock()


class RequestHandler(http.server.BaseHTTPRequestHandler):
    @classmethod
    def decode_basic(cls, data: bytes) -> List[str]:
        decoded_data = base64.b64decode(data).decode("utf-8")
        return decoded_data.split(":", 1)

    def do_AUTHHEAD(self):
        self.send_response(http.HTTPStatus.UNAUTHORIZED)
        self.send_header("WWW-Authenticate", 'Basic realm="Test"')
        self.send_header("Content-type", "text/html")
        self.end_headers()

    def do_ACCESS_GRANTED(self, user: str) -> None:
        self.send_response(http.HTTPStatus.OK)
        response = USER_RESPONSES.get(user)

        if isinstance(response, dict):
            body = json.dumps(response)
        else:
            body = response or ""

        body_raw = body.encode("utf-8")
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body_raw)))
        self.end_headers()
        self.wfile.write(body_raw)

    def reply(self, status, body: str = "") -> None:
        body_raw = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body_raw)))
        self.end_headers()
        self.wfile.write(body_raw)

    def do_directory(self) -> None:
        auth_header = self.headers.get("Authorization") or ""
        if not auth_header.startswith("Basic "):
            self.reply(http.HTTPStatus.UNAUTHORIZED)
            return

        user_name, password = self.decode_basic(auth_header.split(" ", 1)[1])
        with DIRECTORY_LOCK:
            DIRECTORY_COUNTS[user_name] = DIRECTORY_COUNTS.get(user_name, 0) + 1

        if user_name == "tenant_user":
            response = {"roles": TENANT_ROLES.get(self.headers.get("X-Tenant"), [])}
        else:
            response = DIRECTORY_USERS.get(user_name)

        if response is None:
            self.reply(http.HTTPStatus.NOT_FOUND)
        elif password != GOOD_PASSWORD:
            self.reply(http.HTTPStatus.UNAUTHORIZED)
        elif isinstance(response, int):
            self.reply(response)
        else:
            self.reply(http.HTTPStatus.OK, json.dumps(response))

    def do_GET(self):
        if self.path.startswith("/count"):
            user = parse_qs(urlparse(self.path).query).get("user", [""])[0]
            with DIRECTORY_LOCK:
                self.reply(http.HTTPStatus.OK, str(DIRECTORY_COUNTS.get(user, 0)))

        elif self.path == "/directory":
            self.do_directory()

        elif self.path == "/health":
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
            elif self.headers.get("Custom-Header") == "ok" and not self.headers.get(
                "User-Agent"
            ):
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
