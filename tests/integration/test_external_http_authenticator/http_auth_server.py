import base64
import http.server
import json
from typing import List

GOOD_PASSWORD = "good_password"
# See output format examples: 01418_custom_settings.sql
TEST_CASES = {
    "test_user_1": {
        "response": {
            "settings": {
                "auth_str": "test_user",
                "auth_int": 100,
                "auth_signed": -100,
                "auth_float": 100.1,
                "auth_bool": True,
                "auth_false": False,
                "auth_int_min": -(1 << 63),
                "auth_uint_max": (1 << 64) - 1,
                "max_threads": 4,
                "optimize_move_to_prewhere": False,
            }
        },
        "dump_settings": {
            "auth_str": "'test_user'",
            "auth_int": "Int64_100",
            "auth_signed": "Int64_-100",
            "auth_float": "Float64_100.1",
            "auth_bool": "Bool_1",
            "auth_false": "Bool_0",
            "auth_int_min": "Int64_-9223372036854775808",
            "auth_uint_max": "UInt64_18446744073709551615",
        },
        "get_settings": {
            "auth_str": "test_user",
            "auth_int": "100",
            "auth_signed": "-100",
            "auth_float": "100.1",
            "auth_bool": "true",
            "auth_false": "false",
            "auth_int_min": "-9223372036854775808",
            "auth_uint_max": "18446744073709551615",
            "max_threads": "4",
            "optimize_move_to_prewhere": "false",
        },
    },
    "test_user_2": {"response": {}},
    "test_user_3": {"response": ""},
    "test_user_4": {"response": "not json string"},
    "test_extra_legacy": {
        "response": {
            "settings": {
                "auth_str": "'legacy'",
                "auth_num": "UInt64_15",
                "auth_array": "Array_[Int64_1, Int64_2]",
                "max_threads": "4",
            }
        },
        "dump_settings": {
            "auth_str": "'legacy'",
            "auth_num": "UInt64_15",
            "auth_array": "Array_[Int64_1, Int64_2]",
        },
        "get_settings": {
            "auth_str": "legacy",
            "auth_num": "15",
            "auth_array": "[1,2]",
            "max_threads": "4",
        },
    },
    **{
        user: {
            "response": {"settings": {"auth_a": 100, key: value}},
            "absent_settings": ["auth_a"],
        }
        for user, (key, value) in {
            "test_user_5": ("auth_z", {}),
            "test_extra_array": ("auth_z", []),
            "test_extra_null": ("auth_z", None),
            "test_extra_bad_setting": ("max_threads", "abc"),
            "test_extra_uint_overflow": ("auth_z", 1 << 64),
            "test_extra_int_underflow": ("auth_z", -(1 << 63) - 1),
        }.items()
    },
}


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
        response = TEST_CASES.get(user, {}).get("response")

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
