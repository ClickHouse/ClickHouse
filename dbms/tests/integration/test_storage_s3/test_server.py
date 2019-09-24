try:
    from BaseHTTPServer import BaseHTTPRequestHandler
except ImportError:
    from http.server import BaseHTTPRequestHandler

try:
    from BaseHTTPServer import HTTPServer
except ImportError:
    from http.server import HTTPServer

try:
    import urllib.parse as urlparse
except ImportError:
    import urlparse

import json
import logging
import os
import socket
import sys
import threading
import time
import uuid
import xml.etree.ElementTree


logging.getLogger().setLevel(logging.INFO)
file_handler = logging.FileHandler("/var/log/clickhouse-server/test-server.log", "a", encoding="utf-8")
file_handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
logging.getLogger().addHandler(file_handler)
logging.getLogger().addHandler(logging.StreamHandler())

communication_port = int(sys.argv[1])
bucket = sys.argv[2]


def GetFreeTCPPortsAndIP(n):
    result = []
    sockets = []
    for i in range(n):
        tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp.bind((socket.gethostname(), 0))
        addr, port = tcp.getsockname()
        result.append(port)
        sockets.append(tcp)
    [ s.close() for s in sockets ]
    return result, addr

(
    redirecting_to_http_port,
    simple_server_port,
    preserving_data_port,
    multipart_preserving_data_port,
    redirecting_preserving_data_port
), localhost = GetFreeTCPPortsAndIP(5)


data = {
    "redirecting_to_http_port": redirecting_to_http_port,
    "preserving_data_port": preserving_data_port,
    "multipart_preserving_data_port": multipart_preserving_data_port,
    "redirecting_preserving_data_port": redirecting_preserving_data_port,
}


class SimpleHTTPServerHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        logging.info("GET {}".format(self.path))
        if self.path == "/milovidov/test.csv":
             self.send_response(200)
             self.send_header("Content-type", "text/plain")
             self.end_headers()
             data["redirect_csv_data"] = [[42, 87, 44], [55, 33, 81], [1, 0, 9]]
             self.wfile.write("".join([ "{},{},{}\n".format(*row) for row in data["redirect_csv_data"]]))
        else:
             self.send_response(404)
             self.end_headers()
        self.finish()


class RedirectingToHTTPHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(307)
        self.send_header("Content-type", "text/xml")
        self.send_header("Location", "http://{}:{}/milovidov/test.csv".format(localhost, simple_server_port))
        self.end_headers()
        self.wfile.write(r"""<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>TemporaryRedirect</Code>
  <Message>Please re-send this request to the specified temporary endpoint.
  Continue to use the original request endpoint for future requests.</Message>
  <Endpoint>storage.yandexcloud.net</Endpoint>
</Error>""".encode())
        self.finish()


class PreservingDataHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def parse_request(self):
        result = BaseHTTPRequestHandler.parse_request(self)
        # Adaptation to Python 3.
        if sys.version_info.major == 2 and result == True:
            expect = self.headers.get("Expect", "")
            if (expect.lower() == "100-continue" and self.protocol_version >= "HTTP/1.1" and self.request_version >= "HTTP/1.1"):
                if not self.handle_expect_100():
                    return False
        return result

    def send_response_only(self, code, message=None):
        if message is None:
            if code in self.responses:
                message = self.responses[code][0]
            else:
                message = ""
        if self.request_version != "HTTP/0.9":
            self.wfile.write("%s %d %s\r\n" % (self.protocol_version, code, message))

    def handle_expect_100(self):
        logging.info("Received Expect-100")
        self.send_response_only(100)
        self.end_headers()
        return True

    def do_POST(self):
        self.send_response(200)
        query = urlparse.urlparse(self.path).query
        logging.info("PreservingDataHandler POST ?" + query)
        if query == "uploads":
            post_data = r"""<?xml version="1.0" encoding="UTF-8"?>
<hi><UploadId>TEST</UploadId></hi>""".encode()
            self.send_header("Content-length", str(len(post_data)))
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(post_data)
        else:
            post_data = self.rfile.read(int(self.headers.get("Content-Length")))
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            data["received_data_completed"] = True
            data["finalize_data"] = post_data
            data["finalize_data_query"] = query
        self.finish()
 
    def do_PUT(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.send_header("ETag", "hello-etag")
        self.end_headers()
        query = urlparse.urlparse(self.path).query
        path = urlparse.urlparse(self.path).path
        logging.info("Content-Length = " + self.headers.get("Content-Length"))
        logging.info("PUT " + query)
        assert self.headers.get("Content-Length")
        assert self.headers["Expect"] == "100-continue"
        put_data = self.rfile.read()
        data.setdefault("received_data", []).append(put_data)
        logging.info("PUT to {}".format(path))
        self.server.storage[path] = put_data
        self.finish()

    def do_GET(self):
        path = urlparse.urlparse(self.path).path
        if path in self.server.storage:
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.send_header("Content-length", str(len(self.server.storage[path])))
            self.end_headers()
            self.wfile.write(self.server.storage[path])
        else:
            self.send_response(404)
            self.end_headers()
        self.finish()


class MultipartPreservingDataHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def parse_request(self):
        result = BaseHTTPRequestHandler.parse_request(self)
        # Adaptation to Python 3.
        if sys.version_info.major == 2 and result == True:
            expect = self.headers.get("Expect", "")
            if (expect.lower() == "100-continue" and self.protocol_version >= "HTTP/1.1" and self.request_version >= "HTTP/1.1"):
                if not self.handle_expect_100():
                    return False
        return result

    def send_response_only(self, code, message=None):
        if message is None:
            if code in self.responses:
                message = self.responses[code][0]
            else:
                message = ""
        if self.request_version != "HTTP/0.9":
            self.wfile.write("%s %d %s\r\n" % (self.protocol_version, code, message))

    def handle_expect_100(self):
        logging.info("Received Expect-100")
        self.send_response_only(100)
        self.end_headers()
        return True

    def do_POST(self):
        query = urlparse.urlparse(self.path).query
        logging.info("MultipartPreservingDataHandler POST ?" + query)
        if query == "uploads":
            self.send_response(200)
            post_data = r"""<?xml version="1.0" encoding="UTF-8"?>
<hi><UploadId>TEST</UploadId></hi>""".encode()
            self.send_header("Content-length", str(len(post_data)))
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(post_data)
        else:
            try:
                assert query == "uploadId=TEST"
                logging.info("Content-Length = " + self.headers.get("Content-Length"))
                post_data = self.rfile.read(int(self.headers.get("Content-Length")))
                root = xml.etree.ElementTree.fromstring(post_data)
                assert root.tag == "CompleteMultipartUpload"
                assert len(root) > 1
                content = ""
                for i, part in enumerate(root):
                    assert part.tag == "Part"
                    assert len(part) == 2
                    assert part[0].tag == "PartNumber"
                    assert part[1].tag == "ETag"
                    assert int(part[0].text) == i + 1
                    content += self.server.storage["@"+part[1].text]
                data.setdefault("multipart_received_data", []).append(content)
                data["multipart_parts"] = len(root)
                self.send_response(200)
                self.send_header("Content-type", "text/plain")
                self.end_headers()
                logging.info("Sending 200")
            except:
                logging.error("Sending 500")
                self.send_response(500)
        self.finish()
 
    def do_PUT(self):
        uid = uuid.uuid4()
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.send_header("ETag", str(uid))
        self.end_headers()
        query = urlparse.urlparse(self.path).query
        path = urlparse.urlparse(self.path).path
        logging.info("Content-Length = " + self.headers.get("Content-Length"))
        logging.info("PUT " + query)
        assert self.headers.get("Content-Length")
        assert self.headers["Expect"] == "100-continue"
        put_data = self.rfile.read()
        data.setdefault("received_data", []).append(put_data)
        logging.info("PUT to {}".format(path))
        self.server.storage["@"+str(uid)] = put_data
        self.finish()

    def do_GET(self):
        path = urlparse.urlparse(self.path).path
        if path in self.server.storage:
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.send_header("Content-length", str(len(self.server.storage[path])))
            self.end_headers()
            self.wfile.write(self.server.storage[path])
        else:
            self.send_response(404)
            self.end_headers()
        self.finish()


class RedirectingPreservingDataHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def parse_request(self):
        result = BaseHTTPRequestHandler.parse_request(self)
        # Adaptation to Python 3.
        if sys.version_info.major == 2 and result == True:
            expect = self.headers.get("Expect", "")
            if (expect.lower() == "100-continue" and self.protocol_version >= "HTTP/1.1" and self.request_version >= "HTTP/1.1"):
                if not self.handle_expect_100():
                    return False
        return result

    def send_response_only(self, code, message=None):
        if message is None:
            if code in self.responses:
                message = self.responses[code][0]
            else:
                message = ""
        if self.request_version != "HTTP/0.9":
            self.wfile.write("%s %d %s\r\n" % (self.protocol_version, code, message))

    def handle_expect_100(self):
        logging.info("Received Expect-100")
        return True

    def do_POST(self):
        query = urlparse.urlparse(self.path).query
        if query:
            query = "?{}".format(query)
        self.send_response(307)
        self.send_header("Content-type", "text/xml")
        self.send_header("Location", "http://{host}:{port}/{bucket}/test.csv{query}".format(host=localhost, port=preserving_data_port, bucket=bucket, query=query))
        self.end_headers()
        self.wfile.write(r"""<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>TemporaryRedirect</Code>
  <Message>Please re-send this request to the specified temporary endpoint.
  Continue to use the original request endpoint for future requests.</Message>
  <Endpoint>{host}:{port}</Endpoint>
</Error>""".format(host=localhost, port=preserving_data_port).encode())
        self.finish()

    def do_PUT(self):
        query = urlparse.urlparse(self.path).query
        if query:
            query = "?{}".format(query)
        self.send_response(307)
        self.send_header("Content-type", "text/xml")
        self.send_header("Location", "http://{host}:{port}/{bucket}/test.csv{query}".format(host=localhost, port=preserving_data_port, bucket=bucket, query=query))
        self.end_headers()
        self.wfile.write(r"""<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>TemporaryRedirect</Code>
  <Message>Please re-send this request to the specified temporary endpoint.
  Continue to use the original request endpoint for future requests.</Message>
  <Endpoint>{host}:{port}</Endpoint>
</Error>""".format(host=localhost, port=preserving_data_port).encode())
        self.finish()


class CommunicationServerHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(json.dumps(data))
        self.finish()

    def do_PUT(self):
        self.send_response(200)
        self.end_headers()
        logging.info(self.rfile.read())
        self.finish()


servers = []
servers.append(HTTPServer((localhost, communication_port), CommunicationServerHandler))
servers.append(HTTPServer((localhost, redirecting_to_http_port), RedirectingToHTTPHandler))
servers.append(HTTPServer((localhost, preserving_data_port), PreservingDataHandler))
servers[-1].storage = {}
servers.append(HTTPServer((localhost, multipart_preserving_data_port), MultipartPreservingDataHandler))
servers[-1].storage = {}
servers.append(HTTPServer((localhost, simple_server_port), SimpleHTTPServerHandler))
servers.append(HTTPServer((localhost, redirecting_preserving_data_port), RedirectingPreservingDataHandler))
jobs = [ threading.Thread(target=server.serve_forever) for server in servers ]
[ job.start() for job in jobs ]

time.sleep(60) # Timeout

logging.info("Shutting down")
[ server.shutdown() for server in servers ]
logging.info("Joining threads")
[ job.join() for job in jobs ]
logging.info("Done")
