import http.server
import json
import ssl
import sys
import tempfile

CERT = """-----BEGIN CERTIFICATE-----
MIIDCzCCAfOgAwIBAgIUb5QMu4bhLzHO3oWpq9E4xIzZZnwwDQYJKoZIhvcNAQEL
BQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MCAXDTI2MDYxMTE0MzQ1OVoYDzIxMjYw
NTE4MTQzNDU5WjAUMRIwEAYDVQQDDAlsb2NhbGhvc3QwggEiMA0GCSqGSIb3DQEB
AQUAA4IBDwAwggEKAoIBAQCVlM4kbHz/ZpDE66S9VAKlTdTKEIQayC2lYDHf+iS8
DvQ03lItQjYGrKLTFtx3ddPXzm5X18g5WG2Lw5gHgQv4U2a42EGPYkJLoxX883He
zez8jKWk6irykCpQPbfyYZnQMbK9V+NVamu3BucLzeHt/uvQYwvoiClcBw7b69lC
DzuvfpOSjTWtKk+0yA/dCNYMs4QxD7b+jZuRCqNowKnHA4S29xl9LjEYWalwvuct
B/rANup7X+Obf2s5wX/0QZS4Sznjjpnc6hJ/MfBjADWqhZkkOngwxZHqXkd/1W50
zYXMfP0CAAO8xY8VmGJIvPdaLE3Mt//PyRI7F9iuPJlhAgMBAAGjUzBRMB0GA1Ud
DgQWBBRawy47gqguPa1kMb0euvRsUqmF2zAfBgNVHSMEGDAWgBRawy47gqguPa1k
Mb0euvRsUqmF2zAPBgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQB6
RCqz9evGkt+qn8r3BWEoGX83Kp880Eh2tGVBSa40XFAO9K3JX3ZYCaOeYPyyKPYF
ClA8KuW43akkFNGiHdAmqleJZkJkLzS+c+uUwS55DKj4VGtlq43aIeYas5lISblc
JCUxdmlrwi2BiD7J4etRfzQ5iTk1S2b5DxdZ+XeSzaXW+vV2XPDuhjqy1dFqEWBB
3rEx30+fkSJ/dA6GxxqVPPknUQx+2NXn9vikMS1sqa0b3SJ4ZKkJ3rTh6TU/lonx
JrjAVHDzb0JVqs/piwL52CehkRlq/F4enqr7lRZIYyg+sJU8v++GXCTlrJi3Sgl8
xhD25j/IA8LusxXeL55Y
-----END CERTIFICATE-----
"""

KEY = """-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCVlM4kbHz/ZpDE
66S9VAKlTdTKEIQayC2lYDHf+iS8DvQ03lItQjYGrKLTFtx3ddPXzm5X18g5WG2L
w5gHgQv4U2a42EGPYkJLoxX883Hezez8jKWk6irykCpQPbfyYZnQMbK9V+NVamu3
BucLzeHt/uvQYwvoiClcBw7b69lCDzuvfpOSjTWtKk+0yA/dCNYMs4QxD7b+jZuR
CqNowKnHA4S29xl9LjEYWalwvuctB/rANup7X+Obf2s5wX/0QZS4Sznjjpnc6hJ/
MfBjADWqhZkkOngwxZHqXkd/1W50zYXMfP0CAAO8xY8VmGJIvPdaLE3Mt//PyRI7
F9iuPJlhAgMBAAECggEAOYsxcpm5zJcsglUU3zD+g5FZlxKbf6IazVAgX8Xfc+lc
0Snl+ztJhW/Mg+B7mAlgIdlsYabFhCocmnP4fIqMjE+paNro/bwTPP3Ua6dL2ybl
UJLa9oKPWxlS7eOQbFJX1dwIowa4kheKsLKbF9NwYxp6pYQ1BJO9NYYRlVE6F0sm
U7gXH+JQ8chn8xPfcOLz4pXPSoYRer5yPy8/aZyRU25i/9siboP5aZpjWBUWY2J2
jplXgvXqeNdDCuWqbVrqFUa/8jLav631dIcRUllxvCjrVjoWRLbFgTpJb+zKRCbZ
Kr6+SJrtRLIoZBBqYsv7GWqiImae3ZcxXQgU3HrR8QKBgQDIe0LbRfwTnMckdHKX
MH/dCYa8EGpJDsRrYwmh9udg9gIeK3WM8wsIOXXdLm3gPoI028cPodGAzFpf7XNd
yWkWyVPRz7EkL5GRUT6ULSfXfxx/zLwbdlWPnUxCP4nw6n0MK/11Y8AqSJhlbZXs
zKs1zaNSoEFg5TAXhD9NMqMhZwKBgQC/ARLfVLdNpL4MjRlgGAUq1QS3OmH3iLRN
PnAk5LW3SnLv4lILz+P2aPwd89fDtUlboZWv6oTeDKjiLFVj5LBek4hFGRN4AdGm
PHxWeBoK2clxut2PY1jgTkp9XpyzBX12uiCkLXKZkcAZTmdIU/+U5fP3lsmLgABb
S7pZJ4dJ9wKBgGyp8sjrHAB9X6swutOb5BIokbDprNJAgNI78gKp0yvI68jygVqO
eZJRosLp4YBEIUsJPNIKQYXwPaP1DizteFpzcU0tp7QXXG1JfgPOneO97/KNRAAW
mbWn6qeVzOyaDIFGbrDsCkJg6sk/Jp4dKUeWWEn2trkQQIrekXkEU0tNAoGBAJ3A
FkUrY6UVzfzxwCZ0UDg67QUji+v0FO3DBr4Bwu8Z5ummoxqsXVuTA779OJOjs22h
e85pw8jc7dK2yOOS6fOCp8Zh2ol//xXr2MlVsjSKAO0UZ47Yf3vqTW1T6dmVTDT1
rqXJ/19EWELOVVEQRwNEFIXFHLpBQookdkjR89OrAoGAY67Xj8C35SLo4Hyp8IWr
Y+qqhrbmu/2O2TIRiAaZyuvb8LoQOUVsFDH+DYnqHuy/YeZkC/ApMK88x1fSj2x+
ZZZugdqxgnwXZzrxlA1m4Y0mGW6rUBwqhPn376yu8a4vvBIMq0hojbSxXSYAZ0sb
2xQAs9iQ8RIFN+cCiaSGSI0=
-----END PRIVATE KEY-----
"""

requests_log = []


class RequestHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _send(self, code, body, content_type="application/json"):
        data = body.encode()
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path == "/":
            self._send(200, "OK", "text/plain")
        elif self.path == "/hits":
            self._send(200, json.dumps(requests_log))
        else:
            self._send(404, "")

    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0))).decode()
        requests_log.append({"path": self.path, "body": body})
        token = {"access_token": "dummy-token", "expires_in": 3600, "token_type": "Bearer"}
        self._send(200, json.dumps(token))

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    with tempfile.NamedTemporaryFile("w", suffix=".pem") as cert_file, tempfile.NamedTemporaryFile("w", suffix=".pem") as key_file:
        cert_file.write(CERT)
        cert_file.flush()
        key_file.write(KEY)
        key_file.flush()

        server = http.server.ThreadingHTTPServer(("0.0.0.0", int(sys.argv[1])), RequestHandler)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(cert_file.name, key_file.name)
        server.socket = context.wrap_socket(server.socket, server_side=True)
        server.serve_forever()
