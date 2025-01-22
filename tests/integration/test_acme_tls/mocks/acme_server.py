import sys
import datetime
import uuid
import threading
import ssl
import http.server
import json
import base64

from collections import defaultdict

from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.backends import default_backend

HOSTNAME = "localhost:8443"

def generate_self_signed_cert():
    key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
        backend=default_backend()
    )
    utc_now = datetime.datetime.now(datetime.timezone.utc)

    # Generate self-signed certificate
    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
        x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "California"),
        x509.NameAttribute(NameOID.LOCALITY_NAME, "San Francisco"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "ClickHouse"),
        x509.NameAttribute(NameOID.COMMON_NAME, HOSTNAME),
    ])
    cert = x509.CertificateBuilder().subject_name(subject).issuer_name(issuer).public_key(
        key.public_key()).serial_number(x509.random_serial_number()).not_valid_before(
        utc_now).not_valid_after(
        utc_now + datetime.timedelta(days=365)
    ).add_extension(
        x509.SubjectAlternativeName([x509.DNSName(HOSTNAME)]),
        critical=False,
    ).sign(key, hashes.SHA256(), default_backend())

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
    issuer = x509.Name([
        x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
        x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "California"),
        x509.NameAttribute(NameOID.LOCALITY_NAME, "San Francisco"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "ClickHouse"),
        x509.NameAttribute(NameOID.COMMON_NAME, "Integration tests"),
    ])

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
        .add_extension(
            x509.BasicConstraints(ca=False, path_length=None), critical=True
        )
    )

    # Sign the certificate
    cert = cert_builder.sign(private_key=private_key, algorithm=hashes.SHA256())
    return cert

def return_directory():
    return f"""
{{
  "newAccount": "https://{HOSTNAME}/acme/new-acct",
  "newNonce": "https://{HOSTNAME}/acme/new-nonce",
  "newOrder": "https://{HOSTNAME}/acme/new-order",
  "revokeCert": "https://{HOSTNAME}/acme/revoke-cert",
  "wk8GfjnoThY": "https://community.letsencrypt.org/t/adding-random-entries-to-the-directory/33417"
}}
""", 200

NONCE_MAP = defaultdict(bool)
ORDER_MAP = defaultdict(str)
ORDER_MAP[1] = "ready"

CSR_MAP = defaultdict(str)

class RequestHandler(http.server.BaseHTTPRequestHandler):

    def get_response(self):
        if self.path == "/":
            return "OK", 200

        if self.path == "/latest/meta-data/placement/availability-zone":
            return "ci-test-1a", 200

        if self.path == "/directory":
            return return_directory()

        if self.path == "/acme/new-nonce":
            return "", 200

        if self.path == "/acme/authz/1":
            return self._process_authz()

        if self.path == "/acme/order/1":
            return self._describe_order()

        if self.path == "/cert/1":
            private_key, _ = generate_self_signed_cert()

            # For ACME we're using URL-safe base64 encoded bytes
            # No PEM header, no newlines, nothing
            almost_der = CSR_MAP[1]
            almost_der = almost_der.replace("-", "+").replace("_", "/")
            formatted_csr = "\n".join(almost_der[i:i+64] for i in range(0, len(almost_der), 64))
            header = "-----BEGIN CERTIFICATE REQUEST-----"
            footer = "-----END CERTIFICATE REQUEST-----"

            csr = f"{header}\n{formatted_csr}\n{footer}"
            print(csr)

            x509_csr = x509.load_pem_x509_csr(csr.encode())

            certificate = generate_certificate(x509_csr, private_key)
            return certificate.public_bytes(serialization.Encoding.PEM).decode(), 200

        # Resource not found.
        return 404

    def _describe_order(self):
        response = {
            "status": ORDER_MAP[1],
            "expires": "2024-11-11T11:11:11Z",
            "identifiers": [
            {
                "type": "dns",
                "value": "example.com",
            }
            ],
            "authorizations": [f"https://{HOSTNAME}/acme/authz/1"],
            "finalize": f"https://{HOSTNAME}/acme/finalize/1",

            "certificate": f"https://{HOSTNAME}/cert/1",
        }

        return json.dumps(response), 200

    def _process_authz(self):
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
                    "url": f"https://{HOSTNAME}/acme/chall/1",
                    "status": "pending",
                    "token": "token1",
                },
                {
                    "type": "trash-01",
                    "url": f"https://{HOSTNAME}/acme/chall/2",
                    "status": "pending",
                    "token": "token1",
                },
            ]
        }

        return json.dumps(response), 200

    def _new_account(self):
        content_length = int(self.headers['Content-Length'])
        print(content_length)
        post_data = self.rfile.read(content_length)
        request_data = json.loads(post_data)
        print(request_data)

        b64_protected = request_data['protected']
        protected = json.loads(base64.b64decode(b64_protected + '===').decode())
        print(protected)

        nonce = protected['nonce']
        if not NONCE_MAP[nonce]:
            print(f"Nonce not found: {nonce} in map {NONCE_MAP}")
            return "Nonce not found", 400
        NONCE_MAP[nonce] = False

        url = protected['url']
        if url != f"https://{HOSTNAME}/acme/new-acct":
            print(url, f"https://{HOSTNAME}/acme/new-acct")
            return "Invalid URL", 400

        b64_payload = request_data['payload']
        payload = json.loads(base64.b64decode(b64_payload + '===').decode())
        print(payload)

        if payload['termsOfServiceAgreed'] != True:
            return "Terms of service not agreed", 400
        if len(payload['contact']) != 1:
            return "Invalid contact", 400

        response = {
            "key": protected['jwk'],
            "contact": payload['contact'],
            "createdAt": "2024-11-11T11:11:11Z",
            "status": "valid",
        }

        print("Auth OK")

        return json.dumps(response), 201

    def _new_order(self):
        content_length = int(self.headers['Content-Length'])
        print(content_length)
        post_data = self.rfile.read(content_length)
        request_data = json.loads(post_data)
        print(request_data)

        b64_protected = request_data['protected']
        protected = json.loads(base64.b64decode(b64_protected + '===').decode())
        print(protected)

        nonce = protected['nonce']
        if not NONCE_MAP[nonce]:
            print(f"Nonce not found: {nonce} in map {NONCE_MAP}")
            return "Nonce not found", 400
        NONCE_MAP[nonce] = False

        url = protected['url']
        if url != f"https://{HOSTNAME}/acme/new-order":
            print(url, f"https://{HOSTNAME}/acme/new-order")
            return "Invalid URL", 400

        b64_payload = request_data['payload']
        payload = json.loads(base64.b64decode(b64_payload + '===').decode())
        print(payload)

        response = {
            "status": "pending",
            "expires": "2024-11-11T11:11:11Z",
            "identifiers": payload['identifiers'],
            "authorizations": [f"https://{HOSTNAME}/acme/authz/1"],
            "finalize": f"https://{HOSTNAME}/acme/finalize/1",
        }

        print("Order OK")

        return json.dumps(response), 201

    def _process_challenge(self):
        content_length = int(self.headers['Content-Length'])
        print(content_length)
        post_data = self.rfile.read(content_length)
        request_data = json.loads(post_data)
        print(request_data)

        b64_protected = request_data['protected']
        protected = json.loads(base64.b64decode(b64_protected + '===').decode())
        print(protected)

        nonce = protected['nonce']
        if not NONCE_MAP[nonce]:
            print(f"Nonce not found: {nonce} in map {NONCE_MAP}")
            return "Nonce not found", 400
        NONCE_MAP[nonce] = False

        url = protected['url']
        if url != f"https://{HOSTNAME}/acme/chall/1":
            print(url, f"https://{HOSTNAME}/acme/chall/1")
            return "Invalid URL", 400

        response = {
            "status": "valid",
            "url": f"https://{HOSTNAME}/acme/chal/1",
            "token": "token1",
        }

        print("Challenge OK")

        return json.dumps(response), 200

    def _finalize_order(self):
        content_length = int(self.headers['Content-Length'])
        print(content_length)
        post_data = self.rfile.read(content_length)
        request_data = json.loads(post_data)
        print(request_data)

        b64_protected = request_data['protected']
        protected = json.loads(base64.b64decode(b64_protected + '===').decode())
        print(protected)

        nonce = protected['nonce']
        if not NONCE_MAP[nonce]:
            print(f"Nonce not found: {nonce} in map {NONCE_MAP}")
            return "Nonce not found", 400
        NONCE_MAP[nonce] = False

        url = protected['url']
        if url != f"https://{HOSTNAME}/acme/finalize/1":
            print(url, f"https://{HOSTNAME}/acme/finalize/1")
            return "Invalid URL", 400

        b64_payload = request_data['payload']
        payload = json.loads(base64.b64decode(b64_payload + '===').decode())
        print(payload)

        csr = payload['csr']
        CSR_MAP[1] = csr
        ORDER_MAP[1] = "valid"

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
            response, code = self._new_account()
            print(code)
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(response.encode())))
            self.send_header("Location", f"https://{HOSTNAME}/acme/acct/1")
            self.end_headers()
            self.wfile.write(response.encode())

        if self.path == "/acme/new-order":
            response, code = self._new_order()
            print(code)
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(response.encode())))
            self.send_header("Location", f"https://{HOSTNAME}/acme/order/1")
            self.end_headers()
            self.wfile.write(response.encode())

        if self.path == "/acme/chall/1":
            response, code = self._process_challenge()
            print(code)
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(response.encode())))
            self.end_headers()
            self.wfile.write(response.encode())

        if self.path == "/acme/finalize/1":
            response, code = self._finalize_order()
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
        response, _ = self.do_HEAD()
        self.wfile.write(response.encode())


private_key_pem, cert_pem = generate_self_signed_cert()

with open("key.pem", "wb") as key_file:
    key_file.write(private_key_pem)
with open("cert.pem", "wb") as cert_file:
    cert_file.write(cert_pem)

context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
context.load_cert_chain('cert.pem', 'key.pem')

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
