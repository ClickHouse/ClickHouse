import http.server
import sys
import urllib.parse
import xml.etree.ElementTree as ET
from xml.sax.saxutils import escape


OBJECTS = {}


def split_s3_path(path):
    parts = path.lstrip("/").split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""
    return bucket, key


class RequestHandler(http.server.BaseHTTPRequestHandler):
    def is_backup_request(self):
        return self.path.startswith("/backup_session_token") or self.path.startswith(
            "/backup_access_header"
        )

    def check_backup_auth(self):
        if self.path.startswith("/backup_session_token"):
            token = self.headers.get("X-Amz-Security-Token", "")
            if token != "TRUSTED_SESSION_TOKEN":
                self.send_response(403)
                self.end_headers()
                return False

        if self.path.startswith("/backup_access_header"):
            access_header = self.headers.get("X-Backup-Access", "")
            if access_header != "TRUSTED_ACCESS_HEADER":
                self.send_response(403)
                self.end_headers()
                return False

        return True

    def response_body(self):
        if self.path == "/":
            return b"OK"

        revoked_endpoint_header = self.headers.get("X-Revoked-Endpoint-Header", "")
        if "TRUSTED_HEADER" in revoked_endpoint_header:
            return b"7\n"
        authorization = self.headers.get("Authorization", "")
        if "ADMIN_FAKE_KEY" in authorization:
            return b"1\n"
        if "ENV_FAKE_KEY" in authorization:
            return b"3\n"
        if "TRUSTED_FAKE_KEY" in authorization:
            return b"2\n"
        admin_header = self.headers.get("X-Admin-Secret", "")
        if "ADMIN_HEADER" in admin_header:
            return b"4\n"
        if "TRUSTED_HEADER" in admin_header:
            return b"5\n"
        user_header = self.headers.get("X-User-Header", "")
        if "user" in user_header:
            return b"6\n"
        return b"0\n"

    def do_HEAD(self):
        if self.is_backup_request():
            if not self.check_backup_auth():
                return

            bucket, key = split_s3_path(urllib.parse.urlparse(self.path).path)
            data = OBJECTS.get((bucket, key))
            if data is None:
                self.send_response(404)
                self.end_headers()
                return

            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            return

        body = self.response_body()
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()

    def do_GET(self):
        if self.is_backup_request():
            if not self.check_backup_auth():
                return

            parsed = urllib.parse.urlparse(self.path)
            bucket, key = split_s3_path(parsed.path)
            query = urllib.parse.parse_qs(parsed.query)
            if "list-type" in query or "prefix" in query:
                prefix = query.get("prefix", [""])[0]
                contents = []
                for (object_bucket, object_key), data in OBJECTS.items():
                    if object_bucket == bucket and object_key.startswith(prefix):
                        contents.append(
                            f"<Contents><Key>{escape(object_key)}</Key><Size>{len(data)}</Size></Contents>"
                        )
                body = (
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                    "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
                    f"<Name>{escape(bucket)}</Name><Prefix>{escape(prefix)}</Prefix>"
                    f"<KeyCount>{len(contents)}</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>"
                    + "".join(contents)
                    + "</ListBucketResult>"
                ).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/xml")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            data = OBJECTS.get((bucket, key))
            if data is None:
                self.send_response(404)
                self.end_headers()
                return

            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return

        body = self.response_body()
        self.do_HEAD()
        if self.command != "HEAD":
            self.wfile.write(body)

    def do_PUT(self):
        if not self.is_backup_request() or not self.check_backup_auth():
            return

        bucket, key = split_s3_path(urllib.parse.urlparse(self.path).path)
        length = int(self.headers.get("Content-Length", "0"))
        OBJECTS[(bucket, key)] = self.rfile.read(length)
        self.send_response(200)
        self.send_header("ETag", '"mock-etag"')
        self.end_headers()

    def do_DELETE(self):
        if not self.is_backup_request() or not self.check_backup_auth():
            return

        bucket, key = split_s3_path(urllib.parse.urlparse(self.path).path)
        OBJECTS.pop((bucket, key), None)
        self.send_response(204)
        self.end_headers()

    def do_POST(self):
        if not self.is_backup_request() or not self.check_backup_auth():
            return

        parsed = urllib.parse.urlparse(self.path)
        bucket, _ = split_s3_path(parsed.path)
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        if parsed.query == "delete" or "delete" in urllib.parse.parse_qs(parsed.query):
            root = ET.fromstring(body)
            for key_node in root.iter():
                if key_node.tag.endswith("Key") and key_node.text is not None:
                    OBJECTS.pop((bucket, key_node.text), None)

            response = b"<DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"/>"
            self.send_response(200)
            self.send_header("Content-Type", "application/xml")
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response)
            return

        self.send_response(400)
        self.end_headers()

    def log_message(self, format, *args):
        pass


httpd = http.server.HTTPServer(("0.0.0.0", int(sys.argv[1])), RequestHandler)
httpd.serve_forever()
