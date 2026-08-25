import json
import sys
import urllib.parse
from http import server as http_server

counter = 0
expected_path = "/test/test.txt"

# --- Ordinary-contract characterization additions ---
#
# Everything below `expected_path`/`counter` is the original hard-coded single-object mock used by
# test_gcp_auth: unchanged, so that test's token-refresh-count contract stays exactly as it was.
#
# The ordinary-contract test needs more request shapes (PUT, DELETE, LIST, multipart) against
# freely-named objects, plus the ability to inspect what actually reached the wire. `objects` is an
# in-memory bucket keyed by request path; `captured_requests` records every request (method, path,
# lower-cased headers) for the test to fetch and reset independently of the OAuth token counter.
BUCKET_ROOT = "/test/"
objects = {}
generations = {}
multipart_uploads = {}
_next_upload_id = [1]
_next_generation = [1700000000000000]
captured_requests = []


def stable_etag(path):
    """A fixed, path-derived ETag distinct from x-goog-generation, so a test can tell whether the
    response ETag or the generation reached the SDK's ETag field."""
    return "etag-" + path.strip("/").replace("/", "-")


def bump_generation(path):
    _next_generation[0] += 1
    generations[path] = _next_generation[0]
    return generations[path]


class RequestHandler(http_server.BaseHTTPRequestHandler):
    def capture(self):
        captured_requests.append(
            {
                "method": self.command,
                "path": self.path,
                "headers": {name.lower(): value for name, value in self.headers.items()},
            }
        )

    def is_authorized(self):
        current_auth = f"Bearer my-secret-token-{counter}"
        auth = self.headers.get("Authorization")
        return bool(auth) and auth == current_auth

    def read_body(self):
        length = int(self.headers.get("Content-Length", 0) or 0)
        return self.rfile.read(length) if length else b""

    def send_plain(self, status, body=b""):
        self.send_response(status)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body:
            self.wfile.write(body)

    def send_xml(self, status, xml, extra_headers=None):
        encoded = xml.encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/xml")
        self.send_header("Content-Length", str(len(encoded)))
        for name, value in (extra_headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(encoded)

    def process_head(self):
        global counter

        current_auth = f"Bearer my-secret-token-{counter}"
        auth = self.headers.get("Authorization")

        if self.path.endswith("/ping"):
            self.send_response(200)
        elif not auth or auth != current_auth:
            self.send_response(403)
        elif self.path.endswith(expected_path):
            self.send_response(200)
        else:
            self.send_response(404)

        self.send_header("Content-Type", "text/plain")

        if self.path.endswith(expected_path):
            self.send_header("Content-Length", "2")

        self.end_headers()

    def is_original_hardcoded_path(self):
        path_only = self.path.split("?")[0]
        return self.path.endswith("/ping") or path_only == expected_path

    def do_HEAD(self):
        global counter
        self.capture()

        if self.is_original_hardcoded_path():
            self.process_head()
            counter += 1
            return

        path_only = self.path.split("?")[0]
        if not self.is_authorized():
            self.send_plain(403)
            return
        if path_only in objects:
            self.send_response(200)
            self.send_header("ETag", f'"{stable_etag(path_only)}"')
            self.send_header("x-goog-generation", str(generations.get(path_only) or bump_generation(path_only)))
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(objects[path_only])))
            self.end_headers()
        else:
            self.send_plain(404)
        counter += 1

    def do_GET(self):
        global counter

        if self.path.endswith("/reset"):
            # Deliberately does NOT capture(): resetting must stay invisible to /captured, or a test
            # calling reset-then-fetch would see the reset call itself.
            counter = 0
            self.send_plain(200, b"OK")
            return

        if self.path.endswith("/reset_captured"):
            captured_requests.clear()
            self.send_plain(200, b"OK")
            return

        if self.path.endswith("/captured"):
            self.send_response(200)
            body = json.dumps(captured_requests).encode()
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        self.capture()

        if self.path.endswith("/ping"):
            self.send_plain(200, b"OK")
            return

        if not self.is_authorized():
            self.send_plain(403, b"Not authorized")
            return

        parsed = urllib.parse.urlsplit(self.path)
        path_only = parsed.path
        query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)

        if "list-type" in query:
            self.handle_list(query)
            counter += 1
            return

        if path_only == expected_path:
            self.send_response(200)
            self.send_header("ETag", f'"{stable_etag(path_only)}"')
            self.send_header("x-goog-generation", str(generations.get(path_only) or bump_generation(path_only)))
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"OK")
            counter += 1
            return

        if path_only in objects:
            body = objects[path_only]
            self.send_response(200)
            self.send_header("ETag", f'"{stable_etag(path_only)}"')
            self.send_header("x-goog-generation", str(generations.get(path_only) or bump_generation(path_only)))
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            counter += 1
            return

        self.send_plain(404, b"Not found")

    def handle_list(self, query):
        prefix = query.get("prefix", [""])[0]
        full_prefix = BUCKET_ROOT + prefix
        keys = sorted(k for k in objects if k.startswith(full_prefix))
        contents = "".join(
            "<Contents>"
            f"<Key>{key[len(BUCKET_ROOT):]}</Key>"
            f"<ETag>&quot;{stable_etag(key)}&quot;</ETag>"
            f"<Size>{len(objects[key])}</Size>"
            "</Contents>"
            for key in keys
        )
        xml = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            "<ListBucketResult>"
            "<Name>test</Name>"
            f"<Prefix>{prefix}</Prefix>"
            f"<KeyCount>{len(keys)}</KeyCount>"
            "<MaxKeys>1000</MaxKeys>"
            "<IsTruncated>false</IsTruncated>"
            f"{contents}"
            "</ListBucketResult>"
        )
        self.send_xml(200, xml)

    def do_PUT(self):
        self.capture()

        if not self.is_authorized():
            self.send_plain(403)
            self.read_body()
            return

        parsed = urllib.parse.urlsplit(self.path)
        path_only = parsed.path
        query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
        body = self.read_body()

        if "partNumber" in query and "uploadId" in query:
            upload_id = query["uploadId"][0]
            part_number = int(query["partNumber"][0])
            multipart_uploads.setdefault(upload_id, {"path": path_only, "parts": {}})
            multipart_uploads[upload_id]["parts"][part_number] = body
            self.send_response(200)
            self.send_header("ETag", f'"{stable_etag(path_only)}-part-{part_number}"')
            self.send_header("Content-Length", "0")
            self.end_headers()
        else:
            objects[path_only] = body
            generation = bump_generation(path_only)
            self.send_response(200)
            self.send_header("ETag", f'"{stable_etag(path_only)}"')
            self.send_header("x-goog-generation", str(generation))
            self.send_header("Content-Length", "0")
            self.end_headers()
        counter_bump()

    def do_DELETE(self):
        self.capture()

        if not self.is_authorized():
            self.send_plain(403)
            return

        path_only = self.path.split("?")[0]
        objects.pop(path_only, None)
        generations.pop(path_only, None)
        self.send_response(204)
        self.send_header("Content-Length", "0")
        self.end_headers()
        counter_bump()

    def do_POST(self):
        self.capture()

        if not self.is_authorized():
            self.send_plain(403)
            self.read_body()
            return

        parsed = urllib.parse.urlsplit(self.path)
        path_only = parsed.path
        # `keep_blank_values=True` matters here: CreateMultipartUpload's real wire query is the bare
        # flag `?uploads`, with no `=value` -- `parse_qs`'s default drops a key with no value entirely,
        # which silently turned every CreateMultipartUpload into an unmatched 404 until this was traced.
        query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
        self.read_body()

        if "uploads" in query:
            upload_id = f"upload-{_next_upload_id[0]}"
            _next_upload_id[0] += 1
            multipart_uploads[upload_id] = {"path": path_only, "parts": {}}
            xml = (
                '<?xml version="1.0" encoding="UTF-8"?>'
                "<InitiateMultipartUploadResult>"
                "<Bucket>test</Bucket>"
                f"<Key>{path_only[len(BUCKET_ROOT):]}</Key>"
                f"<UploadId>{upload_id}</UploadId>"
                "</InitiateMultipartUploadResult>"
            )
            self.send_xml(200, xml)
        elif "uploadId" in query:
            upload_id = query["uploadId"][0]
            info = multipart_uploads.pop(upload_id, {"path": path_only, "parts": {}})
            full_body = b"".join(info["parts"][part] for part in sorted(info["parts"]))
            objects[path_only] = full_body
            generation = bump_generation(path_only)
            xml = (
                '<?xml version="1.0" encoding="UTF-8"?>'
                "<CompleteMultipartUploadResult>"
                "<Bucket>test</Bucket>"
                f"<Key>{path_only[len(BUCKET_ROOT):]}</Key>"
                f"<ETag>&quot;{stable_etag(path_only)}&quot;</ETag>"
                "</CompleteMultipartUploadResult>"
            )
            self.send_xml(200, xml, extra_headers={"x-goog-generation": str(generation)})
        else:
            self.send_plain(404)
            return
        counter_bump()


def counter_bump():
    global counter
    counter += 1


httpd = http_server.HTTPServer(("0.0.0.0", int(sys.argv[1])), RequestHandler)
httpd.serve_forever()
