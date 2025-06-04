import base64
import datetime
import http.server
import json
import ssl
import sys
import threading
import uuid
import os
import urllib.request
import hashlib

from collections import defaultdict

from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID


HOSTNAME = f"{os.uname().nodename}:8443"


def generate_self_signed_cert():
    key = rsa.generate_private_key(
        public_exponent=65537, key_size=2048, backend=default_backend()
    )
    utc_now = datetime.datetime.now(datetime.timezone.utc)

    # Generate self-signed certificate
    subject = issuer = x509.Name(
        [
            x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
            x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "California"),
            x509.NameAttribute(NameOID.LOCALITY_NAME, "San Francisco"),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "ClickHouse"),
            x509.NameAttribute(NameOID.COMMON_NAME, HOSTNAME),
        ]
    )
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(utc_now)
        .not_valid_after(utc_now + datetime.timedelta(days=365))
        .add_extension(
            x509.SubjectAlternativeName([x509.DNSName(HOSTNAME)]),
            critical=False,
        )
        .sign(key, hashes.SHA256(), default_backend())
    )

    # Serialize key and certificate to memory
    private_key_pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    )
    cert_pem = cert.public_bytes(serialization.Encoding.PEM)

    return private_key_pem, cert_pem


def generate_certificate(csr, issuer_key, days_valid=365):
    private_key = serialization.load_pem_private_key(issuer_key, password=None)

    subject = csr.subject
    issuer = x509.Name(
        [
            x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
            x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "California"),
            x509.NameAttribute(NameOID.LOCALITY_NAME, "San Francisco"),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "ClickHouse"),
            x509.NameAttribute(NameOID.COMMON_NAME, "Integration tests"),
        ]
    )

    utc_now = datetime.datetime.now(datetime.timezone.utc)

    # Build the certificate
    cert_builder = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(csr.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(utc_now)
        .not_valid_after(utc_now + datetime.timedelta(days=days_valid))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
    )

    # Sign the certificate
    cert = cert_builder.sign(private_key=private_key, algorithm=hashes.SHA256())
    return cert


def return_directory():
    return (
        f"""
{{
  "newAccount": "https://{HOSTNAME}/acme/new-acct",
  "newNonce": "https://{HOSTNAME}/acme/new-nonce",
  "newOrder": "https://{HOSTNAME}/acme/new-order",
  "revokeCert": "https://{HOSTNAME}/acme/revoke-cert",
  "wk8GfjnoThY": "https://community.letsencrypt.org/t/adding-random-entries-to-the-directory/33417"
}}
""",
        200,
    )


NONCE_MAP = defaultdict(bool)
ORDER_MAP = defaultdict(str)
CSR_MAP = defaultdict(str)
JWK_MAP = defaultdict(str)

CALL_COUNTERS = defaultdict(int)


class RequestHandler(http.server.BaseHTTPRequestHandler):

    def get_response(self):
        if self.path == "/":
            return "OK", 200

        if self.path == "/directory":
            return return_directory()

        if self.path == "/acme/new-nonce":
            return "", 200

        if self.path.startswith("/acme/authz/"):
            order_id = int(self.path.split("/")[-1])
            if not ORDER_MAP[order_id]:
                return "Order not found", 404

            return self._process_authz(order_id)

        if self.path.startswith("/acme/order/"):
            order_id = int(self.path.split("/")[-1])
            if not ORDER_MAP[order_id]:
                return "Order not found", 404

            return self._describe_order(order_id)

        if self.path.startswith("/cert/"):
            order_id = int(self.path.split("/")[-1])
            if not ORDER_MAP[order_id]:
                return "Order not found", 404

            private_key, _ = generate_self_signed_cert()

            # For ACME we're using URL-safe base64 encoded bytes
            # No PEM header, no newlines, nothing
            almost_der = CSR_MAP[order_id]
            almost_der = almost_der.replace("-", "+").replace("_", "/")
            formatted_csr = "\n".join(
                almost_der[i : i + 64] for i in range(0, len(almost_der), 64)
            )
            header = "-----BEGIN CERTIFICATE REQUEST-----"
            footer = "-----END CERTIFICATE REQUEST-----"

            csr = f"{header}\n{formatted_csr}\n{footer}"
            print(csr)

            x509_csr = x509.load_pem_x509_csr(csr.encode())

            certificate = generate_certificate(x509_csr, private_key)
            return certificate.public_bytes(serialization.Encoding.PEM).decode(), 200

        # Resource not found.
        return 404

    def _describe_order(self, order_id):
        response = {
            "status": ORDER_MAP[order_id],
            "expires": "2024-11-11T11:11:11Z",
            "identifiers": [
                {
                    "type": "dns",
                    "value": "example.com",
                }
            ],
            "authorizations": [f"https://{HOSTNAME}/acme/authz/{order_id}"],
            "finalize": f"https://{HOSTNAME}/acme/finalize/{order_id}",
            "certificate": f"https://{HOSTNAME}/cert/{order_id}",
        }

        return json.dumps(response), 200

    def _process_authz(self, order_id):
        response = {
            "status": "pending",
            "expires": "2024-11-11T11:11:11Z",
            "identifiers": [
                {
                    "type": "dns",
                    "value": "example.com",
                }
            ],
            "challenges": [
                {
                    "type": "http-01",
                    "url": f"https://{HOSTNAME}/acme/chall/{order_id}",
                    "status": "pending",
                    "token": f"token{order_id}",
                },
                {
                    "type": "trash-01",
                    "url": f"https://{HOSTNAME}/acme/chall/{order_id}",
                    "status": "pending",
                    "token": "token12345",
                },
            ],
        }

        return json.dumps(response), 200

    def _new_account(self):
        content_length = int(self.headers["Content-Length"])
        print(content_length)
        post_data = self.rfile.read(content_length)
        request_data = json.loads(post_data)
        print(request_data)

        b64_protected = request_data["protected"]
        protected = json.loads(base64.b64decode(b64_protected + "===").decode())
        print(protected)

        nonce = protected["nonce"]
        if not NONCE_MAP[nonce]:
            print(f"Nonce not found: {nonce} in map {NONCE_MAP}")
            return "Nonce not found", 400
        NONCE_MAP[nonce] = False

        url = protected["url"]
        if url != f"https://{HOSTNAME}/acme/new-acct":
            print(url, f"https://{HOSTNAME}/acme/new-acct")
            return "Invalid URL", 400

        b64_payload = request_data["payload"]
        payload = json.loads(base64.b64decode(b64_payload + "===").decode())
        print(payload)

        if payload["termsOfServiceAgreed"] is not True:
            return "Terms of service not agreed", 400
        if len(payload["contact"]) != 1:
            return "Invalid contact", 400

        json_str = json.dumps(
            protected["jwk"], separators=(",", ":"), ensure_ascii=False
        )
        kid = hashlib.sha256(json_str.encode()).hexdigest()
        JWK_MAP[kid] = json_str

        response = {
            "key": protected["jwk"],
            "contact": payload["contact"],
            "createdAt": "2024-11-11T11:11:11Z",
            "status": "valid",
        }

        print("Auth OK")

        return json.dumps(response), 201, kid

    def _new_order(self):
        content_length = int(self.headers["Content-Length"])
        print(content_length)
        post_data = self.rfile.read(content_length)
        request_data = json.loads(post_data)
        print(request_data)

        b64_protected = request_data["protected"]
        protected = json.loads(base64.b64decode(b64_protected + "===").decode())
        print(protected)

        nonce = protected["nonce"]
        if not NONCE_MAP[nonce]:
            print(f"Nonce not found: {nonce} in map {NONCE_MAP}")
            return "Nonce not found", 400
        NONCE_MAP[nonce] = False

        url = protected["url"]
        if url != f"https://{HOSTNAME}/acme/new-order":
            print(url, f"https://{HOSTNAME}/acme/new-order")
            return "Invalid URL", 400

        b64_payload = request_data["payload"]
        payload = json.loads(base64.b64decode(b64_payload + "===").decode())
        print(payload)

        order_num = len(ORDER_MAP)
        ORDER_MAP[order_num] = "pending"

        response = {
            "status": "pending",
            "expires": "2024-11-11T11:11:11Z",
            "identifiers": payload["identifiers"],
            "authorizations": [f"https://{HOSTNAME}/acme/authz/{order_num}"],
            "finalize": f"https://{HOSTNAME}/acme/finalize/{order_num}",
        }

        print("Order OK")

        return json.dumps(response), 201

    def _process_challenge(self, order_id):
        content_length = int(self.headers["Content-Length"])
        print(content_length)
        post_data = self.rfile.read(content_length)
        request_data = json.loads(post_data)
        print(request_data)

        b64_protected = request_data["protected"]
        protected = json.loads(base64.b64decode(b64_protected + "===").decode())
        print(protected)

        jwk = JWK_MAP[protected["kid"]]

        nonce = protected["nonce"]
        if not NONCE_MAP[nonce]:
            print(f"Nonce not found: {nonce} in map {NONCE_MAP}")
            return "Nonce not found", 400
        NONCE_MAP[nonce] = False

        url = protected["url"]
        if url != f"https://{HOSTNAME}/acme/chall/{order_id}":
            print(url, f"https://{HOSTNAME}/acme/chall/{order_id}")
            return "Invalid URL", 400

        response = {
            "status": "valid",
            "url": f"https://{HOSTNAME}/acme/chal/{order_id}",
            "token": f"token{order_id}",
        }

        request = urllib.request.Request(
            f"http://localhost:8123/.well-known/acme-challenge/token{order_id}"
        )
        with urllib.request.urlopen(request) as res:
            contents = str(res.read().decode())
            print(contents)

            token, hash = contents.split(".")
            if token != f"token{order_id}":
                return f"Invalid token, {token} != token{order_id}", 400

            jwt_sha256 = hashlib.sha256(jwk.encode()).digest()
            base64_jwt_sha256 = (
                base64.urlsafe_b64encode(jwt_sha256).decode().replace("=", "")
            )

            if hash != base64_jwt_sha256:
                return f"Invalid hash, {hash} != {base64_jwt_sha256}, jwk: {jwk}", 400

        ORDER_MAP[order_id] = "ready"

        print("Challenge OK")

        return json.dumps(response), 200

    def _finalize_order(self, order_id):
        content_length = int(self.headers["Content-Length"])
        print(content_length)
        post_data = self.rfile.read(content_length)
        request_data = json.loads(post_data)
        print(request_data)

        b64_protected = request_data["protected"]
        protected = json.loads(base64.b64decode(b64_protected + "===").decode())
        print(protected)

        nonce = protected["nonce"]
        if not NONCE_MAP[nonce]:
            print(f"Nonce not found: {nonce} in map {NONCE_MAP}")
            return "Nonce not found", 400
        NONCE_MAP[nonce] = False

        url = protected["url"]
        if url != f"https://{HOSTNAME}/acme/finalize/{order_id}":
            print(url, f"https://{HOSTNAME}/acme/finalize/{order_id}")
            return "Invalid URL", 400

        b64_payload = request_data["payload"]
        payload = json.loads(base64.b64decode(b64_payload + "===").decode())
        print(payload)

        csr = payload["csr"]
        CSR_MAP[order_id] = csr
        ORDER_MAP[order_id] = "valid"

        response = {
            "status": "ready",
            "expires": "2024-11-11T11:11:11Z",
            "identifiers": [
                {
                    "type": "dns",
                    "value": "example.com",
                }
            ],
            "authorizations": [f"https://{HOSTNAME}/acme/authz/1"],
            "finalize": f"https://{HOSTNAME}/acme/finalize/1",
        }

        print("Finalize OK")

        return json.dumps(response), 200

    def do_POST(self):
        if self.path == "/acme/new-acct":
            CALL_COUNTERS["new_account"] += 1
            response, code, kid = self._new_account()
            print(code)
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(response.encode())))
            self.send_header("Location", kid)
            self.end_headers()
            self.wfile.write(response.encode())

        if self.path == "/acme/new-order":
            CALL_COUNTERS["new_order"] += 1

            response, code = self._new_order()
            print(code)
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(response.encode())))
            self.send_header(
                "Location", f"https://{HOSTNAME}/acme/order/{len(ORDER_MAP) - 1}"
            )
            self.end_headers()
            self.wfile.write(response.encode())

        if self.path.startswith("/acme/chall/"):
            CALL_COUNTERS["process_challenge"] += 1
            order_id = int(self.path.split("/")[-1])
            response, code = self._process_challenge(order_id)
            print(code)
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(response.encode())))
            self.end_headers()
            self.wfile.write(response.encode())

        if self.path.startswith("/acme/finalize/"):
            CALL_COUNTERS["finalize_order"] += 1
            order_id = int(self.path.split("/")[-1])
            response, code = self._finalize_order(order_id)
            print(code)
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(response.encode())))
            self.end_headers()
            self.wfile.write(response.encode())

    def do_HEAD(self):
        response, code = self.get_response()
        self.send_response(code)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(response.encode())))

        if self.path == "/acme/new-nonce":
            random_nonce = str(uuid.uuid4())
            print(random_nonce)
            NONCE_MAP[random_nonce] = True
            self.send_header("Replay-Nonce", str(random_nonce))

        self.end_headers()
        return response, code

    def do_GET(self):
        if self.path == "/counters":
            response = json.dumps(
                {
                    "nonce_count": len(NONCE_MAP),
                    "order_count": len(ORDER_MAP),
                    "csr_count": len(CSR_MAP),
                    "jwk_count": len(JWK_MAP),
                    "call_counters": CALL_COUNTERS,
                }
            )
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(response.encode())))
            self.end_headers()
            self.wfile.write(response.encode())
            return

        response, _ = self.do_HEAD()
        self.wfile.write(response.encode())


if __name__ == "__main__":
    private_key_pem, cert_pem = generate_self_signed_cert()

    with open("key.pem", "wb") as key_file:
        key_file.write(private_key_pem)
    with open("cert.pem", "wb") as cert_file:
        cert_file.write(cert_pem)

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain("cert.pem", "key.pem")

    httpd = http.server.HTTPServer(("0.0.0.0", int(sys.argv[1])), RequestHandler)
    httpd_secure = http.server.ThreadingHTTPServer(("0.0.0.0", 8443), RequestHandler)
    httpd_secure.socket = context.wrap_socket(
        httpd_secure.socket,
        server_side=True,
    )

    t1 = threading.Thread(target=httpd.serve_forever)
    t2 = threading.Thread(target=httpd_secure.serve_forever)

    for t in [t1, t2]:
        t.start()

    for t in [t1, t2]:
        t.join()
