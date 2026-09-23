#!/usr/bin/env python3
"""A mock of the Google OAuth 2.0 token endpoint and of the BigLake Iceberg REST catalog,
sufficient to check service account authentication of the `DataLakeCatalog` database engine:
  - OAuth 2.0 token  POST /token (jwt-bearer grant)
  - catalog          GET /iceberg/v1/config, GET /iceberg/v1/namespaces (an empty catalog)
  - control          GET /__stats__, GET /__reset__

The mock does not verify the JWT signature (it runs inside the ClickHouse container without
third-party Python packages); it records the assertions for the test to verify.
"""

import base64
import http.server
import json
import sys
import urllib.parse

SA_CLIENT_EMAIL = "tester@example-project.iam.gserviceaccount.com"
SA_TOKEN = "test-sa-token"

STATS = {"assertions": [], "catalog_requests": []}


def b64url_decode(data):
    return base64.urlsafe_b64decode(data + "=" * (-len(data) % 4))


class Handler(http.server.BaseHTTPRequestHandler):
    def send_json(self, status, obj):
        body = json.dumps(obj).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)

        if parsed.path == "/":
            body = b"OK"
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if parsed.path == "/__stats__":
            self.send_json(200, STATS)
            return
        if parsed.path == "/__reset__":
            STATS["assertions"].clear()
            STATS["catalog_requests"].clear()
            self.send_json(200, {})
            return

        STATS["catalog_requests"].append(
            {
                "path": parsed.path,
                "authorization": self.headers.get("Authorization", ""),
                "user_project": self.headers.get("x-goog-user-project", ""),
            }
        )

        if self.headers.get("Authorization", "") != f"Bearer {SA_TOKEN}":
            self.send_json(401, {"error": {"code": 401, "message": "unauthenticated"}})
            return
        if parsed.path == "/iceberg/v1/config":
            self.send_json(200, {"defaults": {}, "overrides": {}})
            return
        if parsed.path == "/iceberg/v1/namespaces":
            self.send_json(200, {"namespaces": []})
            return
        self.send_json(
            404, {"error": {"code": 404, "message": f"unexpected GET {parsed.path}"}}
        )

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        body = self.rfile.read(int(self.headers.get("Content-Length", 0))).decode()

        if parsed.path != "/token":
            self.send_json(404, {"error": f"unexpected POST {parsed.path}"})
            return

        params = dict(urllib.parse.parse_qsl(body))
        if params.get("grant_type") != "urn:ietf:params:oauth:grant-type:jwt-bearer":
            self.send_json(400, {"error": "unsupported_grant_type"})
            return

        assertion = params.get("assertion", "")
        STATS["assertions"].append(assertion)
        try:
            claims = json.loads(b64url_decode(assertion.split(".")[1]))
        except Exception as e:
            self.send_json(400, {"error": "invalid_grant", "error_description": str(e)})
            return
        if claims.get("iss") != SA_CLIENT_EMAIL:
            self.send_json(
                400,
                {
                    "error": "invalid_grant",
                    "error_description": "unknown service account",
                },
            )
            return

        self.send_json(
            200, {"access_token": SA_TOKEN, "token_type": "Bearer", "expires_in": 3600}
        )

    def log_message(self, format, *args):
        sys.stderr.write("%s - %s\n" % (self.address_string(), format % args))


if __name__ == "__main__":
    httpd = http.server.ThreadingHTTPServer(("0.0.0.0", int(sys.argv[1])), Handler)
    httpd.serve_forever()
