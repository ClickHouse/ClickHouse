import http.server
import json
import sys


def make_hit(seq):
    return {
        "_index": "partial-response",
        "_id": str(seq),
        "_source": {"seq": seq, "message": f"value-{seq}"},
        "sort": [seq],
    }


class RequestHandler(http.server.BaseHTTPRequestHandler):
    partial_response_sent = False

    def do_GET(self):
        if self.path == "/":
            self.send_json("OK", content_type="text/plain")
            return

        self.send_error(404)

    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(content_length) or b"{}")

        if self.path != "/partial-response/_search":
            self.send_error(404)
            return

        if not RequestHandler.partial_response_sent:
            RequestHandler.partial_response_sent = True
            self.send_json(
                {
                    "timed_out": True,
                    "_shards": {"total": 1, "successful": 1, "failed": 0},
                    "hits": {"hits": [make_hit(2)]},
                }
            )
            return

        hits = [] if body.get("search_after") else [make_hit(1), make_hit(2)]
        self.send_json(
            {
                "timed_out": False,
                "_shards": {"total": 1, "successful": 1, "failed": 0},
                "hits": {"hits": hits},
            }
        )

    def send_json(self, value, content_type="application/json"):
        body = value.encode() if isinstance(value, str) else json.dumps(value).encode()
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass


http.server.ThreadingHTTPServer(
    ("0.0.0.0", int(sys.argv[1])), RequestHandler
).serve_forever()
