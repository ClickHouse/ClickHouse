import base64
import http.server
import json
import time
from typing import List

GOOD_PASSWORD = "good_password"
USER_RESPONSES = {
    "test_user_1": {"settings": {"auth_user": "'test_user'", "auth_num": "UInt64_15"}},
    "test_user_2": {},
    "test_user_3": "",
    "test_user_4": "not json string",
    "role_user": {"roles": ["http_reader"]},
    "multi_role_user": {"roles": ["role_a", "role_b"]},
    "unknown_role_user": {"roles": ["http_reader", "role_that_does_not_exist"]},
    "no_roles_user": {},
    "malformed_roles_type_user": {
        "settings": {"auth_num": "UInt64_15"},
        "roles": "http_reader",
    },
    "malformed_roles_number_user": {
        "settings": {"auth_num": "UInt64_15"},
        "roles": ["http_reader", 123],
    },
    "malformed_roles_bool_user": {
        "settings": {"auth_num": "UInt64_15"},
        "roles": ["http_reader", True],
    },
    "partial_settings_user": {
        "settings": {
            "auth_a_valid": "UInt64_15",
            "auth_z_invalid": "UInt64_not-a-number",
        },
        "roles": ["http_reader"],
    },
    "malformed_settings_user": {
        "settings": {"auth_num": "UInt64_not-a-number"},
        "roles": ["http_reader"],
    },
    "expiry_zero_user": {"valid_until": 0},
    "expiry_string_user": {"valid_until": "not-a-timestamp"},
    "expiry_bool_user": {"valid_until": True},
    "expiry_fraction_user": {"valid_until": 1.5},
    "expiry_out_of_range_user": {"valid_until": 9223372036854775808},
    "interserver_user": {"roles": ["helper_reader"]},
    "interserver_unknown_user": {"roles": ["initiator_only_role"]},
    "external_role_settings_user": {"roles": ["external_role_with_profile"]},
}


def get_response(user: str, request: http.server.BaseHTTPRequestHandler):
    if user == "expiry_future_user":
        return {"valid_until": int(time.time()) + 15}
    if user == "expiry_past_user":
        return {"roles": ["http_reader"], "valid_until": int(time.time()) - 30}
    if user == "expiry_later_user":
        return {"valid_until": int(time.time()) + 60}
    if user == "named_session_user":
        if request.headers.get("Custom-Header") == "roles-reader":
            return {"roles": ["named_session_reader"]}
        if request.headers.get("Custom-Header") == "roles-none":
            return {"roles": []}
    if user == "named_session_limits_user":
        if request.headers.get("Custom-Header") == "roles-unlimited":
            return {"roles": ["named_session_unlimited"]}
        if request.headers.get("Custom-Header") == "roles-limited":
            return {"roles": ["named_session_limited"]}
    return USER_RESPONSES.get(user)


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
        response = get_response(user, self)

        if isinstance(response, dict):
            body = json.dumps(response)
        else:
            body = response or ""

        body_raw = body.encode("utf-8")
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body_raw)))
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
