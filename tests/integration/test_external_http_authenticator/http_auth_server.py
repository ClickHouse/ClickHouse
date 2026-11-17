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

# Behaviour of the `/directory` endpoint (used by the `http` user directory tests), by user name, or by `user/tenant`
# with the `X-Tenant` header. A `str` is sent verbatim as the 200 body, an `int` is a status.
DIRECTORY_USERS = {
    "alice": {"settings": {"max_threads": "3"}, "roles": ["r_reader"]},
    "plain": {},
    "dist_user": {"roles": ["r_reader"]},
    "tenant_user": {},
    "tenant_user/a": {"roles": ["r_a"]},
    "tenant_user/b": {"roles": ["r_b"]},
    "tenant_user/imp": {"roles": ["r_imp"]},
    "ro_session": {"settings": {"readonly": "1"}, "roles": ["r_reader"]},
    "ro_session/x": {"settings": {"readonly": "1", "max_threads": "2"}, "roles": ["r_reader"]},
    "default_setting": {"settings": {"readonly": "0"}, "roles": ["r_reader"]},
    "unsettable": {"settings": {"max_sessions_for_user": "100"}, "roles": ["r_reader"]},
    "bad_role": {"roles": ["r_admin"]},
    "ghost_role": {"roles": ["tenant_missing"]},
    "ro_user": {"roles": ["r_ro"]},
    "prof_user": {"roles": ["r_prof"]},
    "bad_setting": {"settings": {"nonexistent_setting": "UInt64_1"}},
    "bad_json": '{"roles": [',
    "not_object": '["r_reader"]',
    "roles_not_array": {"roles": "r_reader"},
    "role_not_string": {"roles": [1]},
    "settings_not_object": {"settings": ["readonly"]},
    "bad_value": {"settings": {"max_threads": "abc"}},
    "broken": 500,
}
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
        user_name, password = self.decode_basic(self.headers["Authorization"].split(" ", 1)[1])
        with DIRECTORY_LOCK:
            DIRECTORY_COUNTS[user_name] = DIRECTORY_COUNTS.get(user_name, 0) + 1

        tenant = self.headers.get("X-Tenant")
        response = DIRECTORY_USERS.get(f"{user_name}/{tenant}" if tenant else user_name)
        if response is None:
            self.reply(http.HTTPStatus.NOT_FOUND)
        elif password != GOOD_PASSWORD:
            self.reply(http.HTTPStatus.UNAUTHORIZED)
        elif isinstance(response, int):
            self.reply(response)
        else:
            self.reply(http.HTTPStatus.OK, response if isinstance(response, str) else json.dumps(response))

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

        else:
            self.reply(http.HTTPStatus.NOT_FOUND)


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
