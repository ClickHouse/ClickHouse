import pytest

from helpers.cluster import ClickHouseCluster

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        instance = cluster.add_instance('dummy')
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


import os
import socket
import subprocess
import sys
import tempfile
import threading
import time


try:
    import urllib.parse as urlparse
except ImportError:
    import urlparse

try:
    from BaseHTTPServer import BaseHTTPRequestHandler
except ImportError:
    from http.server import BaseHTTPRequestHandler

try:
    from BaseHTTPServer import HTTPServer
except ImportError:
    from http.server import HTTPServer


received_data = []
received_data_completed = False
    
    
def test_sophisticated_default(started_cluster):
    instance = started_cluster.instances['dummy']

    def GetCurrentIP():
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip

    localhost = GetCurrentIP()

    def GetFreeTCPPorts(n):
        result = []
        sockets = []
        for i in range(n):
            tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            tcp.bind((localhost, 0))
            addr, port = tcp.getsockname()
            result.append(port)
            sockets.append(tcp)
        [ s.close() for s in sockets ]
        return result

    format = 'column1 UInt32, column2 UInt32, column3 UInt32'
    values = '(1, 2, 3), (3, 2, 1), (78, 43, 45)'
    other_values = '(1, 1, 1), (1, 1, 1), (11, 11, 11)'
    redirecting_host = localhost
    redirecting_to_http_port, redirecting_to_https_port, preserving_data_port, redirecting_preserving_data_port = GetFreeTCPPorts(4)
    bucket = 'abc'

    def run_query(query):
        print('Running query "{}"...'.format(query))
        result = instance.query(query)
        print('Query finished')
        return result
    
    
    prepare_put_queries = [
        "insert into table function s3('http://{}:{}/{}/test.csv', 'CSV', '{}') values {}".format(localhost, preserving_data_port, bucket, format, values),
    ]
    
    queries = [
        "select *, column1*column2*column3 from s3('http://{}:{}/', 'CSV', '{}')".format(redirecting_host, redirecting_to_http_port, format),
#        "select *, column1*column2*column3 from s3('http://{}:{}/', 'CSV', '{}')".format(redirecting_host, redirecting_to_https_port, format),
# FIXME
    ]
    
    put_query = "insert into table function s3('http://{}:{}/{}/test.csv', 'CSV', '{}') values {}".format(redirecting_host, preserving_data_port, bucket, format, values)
    
    redirect_put_query = "insert into table function s3('http://{}:{}/{}/test.csv', 'CSV', '{}') values {}".format(redirecting_host, redirecting_preserving_data_port, bucket, format, other_values)
    
    check_queries = [
        "select *, column1*column2*column3 from s3('http://{}:{}/{}/test.csv', 'CSV', '{}')".format(localhost, preserving_data_port, bucket, format),
    ]
    
    
    class RedirectingToHTTPHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(307)
            self.send_header('Content-type', 'text/xml')
            self.send_header('Location', 'http://storage.yandexcloud.net/milovidov/test.csv')
            self.end_headers()
            self.wfile.write(r'''<?xml version="1.0" encoding="UTF-8"?>
    <Error>
      <Code>TemporaryRedirect</Code>
      <Message>Please re-send this request to the specified temporary endpoint.
      Continue to use the original request endpoint for future requests.</Message>
      <Endpoint>storage.yandexcloud.net</Endpoint>
    </Error>'''.encode())
            self.finish()
    
    
    class RedirectingToHTTPSHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(307)
            self.send_header('Content-type', 'text/xml')
            self.send_header('Location', 'https://storage.yandexcloud.net/milovidov/test.csv')
            self.end_headers()
            self.wfile.write(r'''<?xml version="1.0" encoding="UTF-8"?>
    <Error>
      <Code>TemporaryRedirect</Code>
      <Message>Please re-send this request to the specified temporary endpoint.
      Continue to use the original request endpoint for future requests.</Message>
      <Endpoint>storage.yandexcloud.net</Endpoint>
    </Error>'''.encode())
            self.finish()
    
    
    class PreservingDataHandler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'

        def parse_request(self):
            result = BaseHTTPRequestHandler.parse_request(self)
            # Adaptation to Python 3.
            if sys.version_info.major == 2 and result == True:
                expect = self.headers.get('Expect', "")
                if (expect.lower() == "100-continue" and self.protocol_version >= "HTTP/1.1" and self.request_version >= "HTTP/1.1"):
                    if not self.handle_expect_100():
                        return False
            return result

        def send_response_only(self, code, message=None):
            if message is None:
                if code in self.responses:
                    message = self.responses[code][0]
                else:
                    message = ''
            if self.request_version != 'HTTP/0.9':
                self.wfile.write("%s %d %s\r\n" % (self.protocol_version, code, message))

        def handle_expect_100(self):
            # FIXME it does not work in Python 2. :(
            print('Received Expect-100')
            self.send_response_only(100)
            self.end_headers()
            return True
    
        def do_POST(self):
            self.send_response(200)
            query = urlparse.urlparse(self.path).query
            print('POST', query)
            if query == 'uploads':
                data = r'''<?xml version="1.0" encoding="UTF-8"?>
    <hi><UploadId>TEST</UploadId></hi>'''.encode()
                self.send_header('Content-length', str(len(data)))
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
                self.wfile.write(data)
            else:
                data = self.rfile.read(int(self.headers.get('Content-Length')))
                assert query == 'uploadId=TEST'
                assert data == b'<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>hello-etag</ETag></Part></CompleteMultipartUpload>'
                self.send_header('Content-length', '0') # FIXME on python2 somehow connection does not close without this
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
                global received_data_completed
                received_data_completed = True
            self.finish()
     
        def do_PUT(self):
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.send_header('ETag', 'hello-etag')
            self.end_headers()
            query = urlparse.urlparse(self.path).query
            path = urlparse.urlparse(self.path).path
            print('Content-Length =', self.headers.get('Content-Length'))
            print('PUT', query)
            assert self.headers.get('Content-Length')
            assert self.headers['Expect'] == '100-continue'
            data = self.rfile.read()
            received_data.append(data)
            print('PUT to {}'.format(path))
            self.server.storage[path] = data
            self.finish()
    
        def do_GET(self):
            path = urlparse.urlparse(self.path).path
            if path in self.server.storage:
                self.send_response(200)
                self.send_header('Content-type', 'text/plain')
                self.send_header('Content-length', str(len(self.server.storage[path])))
                self.end_headers()
                self.wfile.write(self.server.storage[path])
            else:
                self.send_response(404)
                self.end_headers()
            self.finish()
    
    
    class RedirectingPreservingDataHandler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'

        def parse_request(self):
            result = BaseHTTPRequestHandler.parse_request(self)
            # Adaptation to Python 3.
            if sys.version_info.major == 2 and result == True:
                expect = self.headers.get('Expect', "")
                if (expect.lower() == "100-continue" and self.protocol_version >= "HTTP/1.1" and self.request_version >= "HTTP/1.1"):
                    if not self.handle_expect_100():
                        return False
            return result

        def send_response_only(self, code, message=None):
            if message is None:
                if code in self.responses:
                    message = self.responses[code][0]
                else:
                    message = ''
            if self.request_version != 'HTTP/0.9':
                self.wfile.write("%s %d %s\r\n" % (self.protocol_version, code, message))

        def handle_expect_100(self):
            print('Received Expect-100')
            return True
    
        def do_POST(self):
            query = urlparse.urlparse(self.path).query
            if query:
                query = '?{}'.format(query)
            self.send_response(307)
            self.send_header('Content-type', 'text/xml')
            self.send_header('Location', 'http://{host}:{port}/{bucket}/test.csv{query}'.format(host=localhost, port=preserving_data_port, bucket=bucket, query=query))
            self.end_headers()
            self.wfile.write(r'''<?xml version="1.0" encoding="UTF-8"?>
    <Error>
      <Code>TemporaryRedirect</Code>
      <Message>Please re-send this request to the specified temporary endpoint.
      Continue to use the original request endpoint for future requests.</Message>
      <Endpoint>{host}:{port}</Endpoint>
    </Error>'''.format(host=localhost, port=preserving_data_port).encode())
            self.finish()
    
        def do_PUT(self):
            query = urlparse.urlparse(self.path).query
            if query:
                query = '?{}'.format(query)
            self.send_response(307)
            self.send_header('Content-type', 'text/xml')
            self.send_header('Location', 'http://{host}:{port}/{bucket}/test.csv{query}'.format(host=localhost, port=preserving_data_port, bucket=bucket, query=query))
            self.end_headers()
            self.wfile.write(r'''<?xml version="1.0" encoding="UTF-8"?>
    <Error>
      <Code>TemporaryRedirect</Code>
      <Message>Please re-send this request to the specified temporary endpoint.
      Continue to use the original request endpoint for future requests.</Message>
      <Endpoint>{host}:{port}</Endpoint>
    </Error>'''.format(host=localhost, port=preserving_data_port).encode())
            self.finish()
    
    
    servers = []
    servers.append(HTTPServer((redirecting_host, redirecting_to_https_port), RedirectingToHTTPSHandler))
    servers.append(HTTPServer((redirecting_host, redirecting_to_http_port), RedirectingToHTTPHandler))
    servers.append(HTTPServer((redirecting_host, preserving_data_port), PreservingDataHandler))
    servers[-1].storage = {}
    servers.append(HTTPServer((redirecting_host, redirecting_preserving_data_port), RedirectingPreservingDataHandler))
    jobs = [ threading.Thread(target=server.serve_forever) for server in servers ]
    [ job.start() for job in jobs ]
    
    try:
        print('Phase 1')
        for query in prepare_put_queries:
            run_query(query)
    
        print('Phase 2')
        for query in queries:
            stdout = run_query(query)
            assert list(map(str.split, stdout.splitlines())) == [
                ['1', '2', '3', '6'],
                ['3', '2', '1', '6'],
                ['78', '43', '45', '150930'],
            ]
    
        print('Phase 3')
        query = put_query
        global received_data_completed
        received_data_completed = False
        run_query(query)
        assert received_data[-1].decode() == '1,2,3\n3,2,1\n78,43,45\n'
        assert received_data_completed
    
        print('Phase 4')
        query = redirect_put_query
        run_query(query)
    
        for query in check_queries:
            print(query)
            stdout = run_query(query)
            assert list(map(str.split, stdout.splitlines())) == [
                ['1', '1', '1', '1'],
                ['1', '1', '1', '1'],
                ['11', '11', '11', '1331'],
            ]
    
    finally:
        print('Shutting down')
        [ server.shutdown() for server in servers ]
        print('Joining threads')
        [ job.join() for job in jobs ]
        print('Done')
