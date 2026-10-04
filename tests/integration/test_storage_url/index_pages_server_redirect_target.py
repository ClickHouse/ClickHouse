import sys
from http.server import HTTPServer

from index_pages_server import RequestHandler


if __name__ == "__main__":
    port = int(sys.argv[1])
    httpd = HTTPServer(("0.0.0.0", port), RequestHandler)
    httpd.serve_forever()
