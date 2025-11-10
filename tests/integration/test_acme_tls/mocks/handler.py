import http.server
import uuid
import json

from collections import defaultdict

from handlers.handle_authz import _process_authz
from handlers.describe_order import _describe_order
from handlers.handle_cert_request import _handle_certificate_get_request
from handlers.new_account import _new_account
from handlers.new_order import _new_order
from handlers.process_challenge import _process_challenge
from handlers.finalize_order import _finalize_order


class RequestHandler(http.server.BaseHTTPRequestHandler):
    NONCE_MAP = defaultdict(bool)
    ORDER_MAP = defaultdict(str)
    CSR_MAP = defaultdict(str)
    JWK_MAP = defaultdict(str)

    CALL_COUNTERS = defaultdict(int)

    HOSTNAME = "did it work?"

    def get_response(self):
        routes = {
            "/": lambda: ("OK", 200),
            "/acme/new-nonce": lambda: ("", 200),
            "/directory": lambda: (self.return_directory(), 200),
            "/acme/authz/*": self._handle_authz,
            "/acme/order/*": self._handle_order,
            "/cert/*": self._handle_cert_request,
        }

        for route, handler in routes.items():
            if route.endswith("*"):
                if self.path.startswith(route[:-1]):
                    return handler()
            elif self.path == route:
                return handler()

        return ("Not Found", 404)

    def return_directory(self):
        return (
            f"""
{{
  "newAccount": "https://{self.HOSTNAME}/acme/new-acct",
  "newNonce": "https://{self.HOSTNAME}/acme/new-nonce",
  "newOrder": "https://{self.HOSTNAME}/acme/new-order",
  "revokeCert": "https://{self.HOSTNAME}/acme/revoke-cert",
  "wk8GfjnoThY": "https://community.letsencrypt.org/t/adding-random-entries-to-the-directory/33417"
}}
    """,
            200,
        )

    def _handle_authz(self):
        order_id = int(self.path.split("/")[-1])
        if not self.ORDER_MAP[order_id]:
            return "Order not found", 404

        return _process_authz(self.HOSTNAME, order_id)

    def _handle_order(self):
        order_id = int(self.path.split("/")[-1])
        if not self.ORDER_MAP[order_id]:
            return "Order not found", 404

        return _describe_order(self.ORDER_MAP, hostname, order_id)

    def _handle_cert_request(self):
        order_id = int(self.path.split("/")[-1])
        if not self.ORDER_MAP[order_id]:
            return "Order not found", 404

        return _handle_certificate_get_request(self.CSR_MAP, self.HOSTNAME, order_id)

    def do_POST(self):
        routes = {
            "/acme/new-acct": self._handle_new_account,
            "/acme/new-order": self._handle_new_order,
            "/acme/chall/*": self._handle_challenge,
            "/acme/finalize/*": self._handle_finalize,
        }

        for route, handler in routes.items():
            if route.endswith("*"):
                if self.path.startswith(route[:-1]):
                    handler()
                    return
            elif self.path == route:
                handler()
                return

    def _handle_new_account(self):
        self.CALL_COUNTERS["new_account"] += 1

        request_data = self._read_post_json()

        response, code, kid = _new_account(
            self.NONCE_MAP,
            self.JWK_MAP,
            self.HOSTNAME,
            request_data
        )
        print(code)
        self._send_json_response(response, code, extra_headers={"Location": kid})

    def _handle_new_order(self):
        self.CALL_COUNTERS["new_order"] += 1

        request_data = self._read_post_json()

        response, code = _new_order(
            self.ORDER_MAP,
            self.NONCE_MAP,
            self.HOSTNAME,
            request_data
        )
        print(code)

        self._send_json_response(response, code, extra_headers={
            "Location": f"https://{self.HOSTNAME}/acme/order/{len(self.ORDER_MAP) - 1}"
        })

    def _handle_challenge(self):
        self.CALL_COUNTERS["process_challenge"] += 1

        request_data = self._read_post_json()

        order_id = int(self.path.split("/")[-1])
        response, code = _process_challenge(
            self.JWK_MAP,
            self.NONCE_MAP,
            self.HOSTNAME,
            request_data,
            order_id
        )
        print(code)

        if code == 200:
            self.ORDER_MAP[order_id] = "ready"

        self._send_json_response(response, code)

    def _handle_finalize(self):
        self.CALL_COUNTERS["finalize_order"] += 1

        request_data = self._read_post_json()

        order_id = int(self.path.split("/")[-1])
        response, code, csr = _finalize_order(
            self.NONCE_MAP,
            self.HOSTNAME,
            request_data,
            order_id
        )
        print(code)

        if code == 200:
            self.CSR_MAP[order_id] = csr
            self.ORDER_MAP[order_id] = "valid"

        self._send_json_response(response, code)

    def do_HEAD(self):
        response, code = self.get_response()
        self.send_response(code)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(response.encode())))

        if self.path == "/acme/new-nonce":
            random_nonce = str(uuid.uuid4())
            print(random_nonce)
            self.NONCE_MAP[random_nonce] = True
            self.send_header("Replay-Nonce", str(random_nonce))

        self.end_headers()
        return response, code

    def do_GET(self):
        if self.path == "/counters":
            response = json.dumps(
                {
                    "nonce_count": len(self.NONCE_MAP),
                    "order_count": len(self.ORDER_MAP),
                    "csr_count": len(self.CSR_MAP),
                    "jwk_count": len(self.JWK_MAP),
                    "call_counters": self.CALL_COUNTERS,
                }
            )
            self._send_json_response(response, 200)
            return

        response, _ = self.do_HEAD()
        self.wfile.write(response.encode())

    def _read_post_json(self):
        content_length = int(self.headers["Content-Length"])
        print(content_length)
        post_data = self.rfile.read(content_length)
        request_data = json.loads(post_data)
        print(request_data)
        return request_data

    def _send_json_response(self, response, code, extra_headers=None):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response.encode())))
        if extra_headers:
            for key, value in extra_headers.items():
                self.send_header(key, value)
        self.end_headers()
        self.wfile.write(response.encode())
