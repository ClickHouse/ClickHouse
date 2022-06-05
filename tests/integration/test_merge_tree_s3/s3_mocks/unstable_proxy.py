import argparse
import hashlib
import hmac
import http.client
import http.server
import random
import socketserver
import sys
import urllib.parse


parser = argparse.ArgumentParser()
parser.add_argument("port", type=int, help="port to listen")
parser.add_argument("upstream", type=str, help="upstream to redirect requests to")
args = parser.parse_args()

random.seed("Unstable proxy/1.0")
key = None


def request(command, url, headers={}, data=b""):
    """Mini-requests."""

    class Dummy:
        pass

    parts = urllib.parse.urlparse(url)
    c = http.client.HTTPConnection(parts.hostname, parts.port)
    c.request(
        command,
        urllib.parse.urlunparse(parts._replace(scheme="", netloc="")),
        headers=headers,
        body=data,
    )
    r = c.getresponse()
    result = Dummy()
    result.status_code = r.status
    result.headers = r.headers
    result.content = r.read()
    return result


class RequestHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        global key
        parts = urllib.parse.urlparse(self.path)
        if self.path == "/" or parts.path == "/":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK")
            params = urllib.parse.parse_qs(parts.query)
            if "key" in params:
                key, = params["key"]
        else:
            self.do_HEAD()

    def do_PUT(self):
        self.do_HEAD()

    def do_POST(self):
        self.do_HEAD()

    def sign(self, key, msg):
        return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

    def get_signing_key(self, key, date, region, service):
        k_date = self.sign(("AWS4" + key).encode("utf-8"), date)
        k_region = self.sign(k_date, region)
        k_service = self.sign(k_region, service)
        return self.sign(k_service, "aws4_request")

    def replace_authorization(self, authorization):
        method, credential, signed_headers, signature = authorization.split(" ")
        assert method == "AWS4-HMAC-SHA256"
        assert credential.startswith("Credential=")
        assert signed_headers.startswith("SignedHeaders=")
        assert signature.startswith("Signature=")
        _, credential = credential.split("=")
        credential, _ = credential.split(",")
        _, signature = signature.split("=")
        _, signed_headers = signed_headers.split("=")
        signed_headers, _ = signed_headers.split(",")
        key_id, date, region, service, aws4_request = credential.split("/")
        assert service == "s3"
        assert aws4_request == "aws4_request"
        signing_key = self.get_signing_key(key, date, region, service)
        amz_date = self.headers["x-amz-date"]
        parts = urllib.parse.urlparse(self.path)
        canonical_uri = parts.path
        canonical_querystring = urllib.parse.urlencode(dict(sorted(((k, v) for k, (v,) in urllib.parse.parse_qs(parts.query, keep_blank_values=True).items()))))
        canonical_headers = "\n".join((f"{h}:{self.headers[h]}" for h in signed_headers.split(";"))) + "\n"
        payload_hash = hashlib.sha256(self.data).hexdigest()
        canonical_request = "\n".join([self.command, canonical_uri, canonical_querystring, canonical_headers, signed_headers, payload_hash])
        credential_scope = "/".join([date, region, service, aws4_request])
        string_to_sign = "\n".join([method, amz_date, credential_scope, hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()])
        expect_signature = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
        assert signature == expect_signature, f"Signatures are different: '{signature}' != '{expect_signature}'"
        canonical_headers = "\n".join((f"{h}:{self.headers[h] if h != 'host' else args.upstream}" for h in signed_headers.split(";"))) + "\n"
        canonical_request = "\n".join([self.command, canonical_uri, canonical_querystring, canonical_headers, signed_headers, payload_hash])
        string_to_sign = "\n".join([method, amz_date, credential_scope, hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()])
        signature = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
        new_authorization = f"{method} Credential={key_id}/{date}/{region}/s3/aws4_request, SignedHeaders={signed_headers}, Signature={signature}"
        return new_authorization

    def replace_headers(self, headers):
        result = {}
        for k, v in headers.items():
            if k.lower() == "host":
                result[k] = args.upstream
            elif k.lower() == "authorization":
                result[k] = self.replace_authorization(v) if key else v
            else:
                result[k] = v
        return result

    def do_HEAD(self):
        content_length = self.headers.get("Content-Length")
        self.data = self.rfile.read(int(content_length)) if content_length else b""
        r = request(
            self.command,
            f"http://{args.upstream}{self.path}",
            headers=self.replace_headers(self.headers),
            data=self.data,
        )
        self.send_response(r.status_code)
        for k, v in r.headers.items():
            self.send_header(k, v)
        self.end_headers()
        if random.random() < 0.25 and len(r.content) > 1024 * 1024:
            r.content = r.content[: len(r.content) // 2]
        self.wfile.write(r.content)
        self.wfile.flush()


class ThreadedHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    """Handle requests in a separate thread."""


httpd = ThreadedHTTPServer(("0.0.0.0", args.port), RequestHandler)
httpd.serve_forever()
