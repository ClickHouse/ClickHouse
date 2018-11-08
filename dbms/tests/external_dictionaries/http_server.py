#!/usr/bin/env python
import os
import socket
import sys
import BaseHTTPServer
import SocketServer
import ssl
import argparse

parser = argparse.ArgumentParser(description = 'Simple http/https server')
parser.add_argument('--https', action='store_true', help = 'Use https')
parser.add_argument('--port', type = int, default = 80, help = 'server port')
parser.add_argument('--host', default = "localhost", help = 'server host')
args = parser.parse_args()

if args.https and args.port == 80:
    args.port = 443

class myHTTPServer(SocketServer.ForkingMixIn, BaseHTTPServer.HTTPServer):
    address_family = socket.AF_INET6
    pass

class myHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/":
            self.path = "/http_server.py"

        try:
            f = open(os.curdir + os.sep + self.path)
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(f.read())
            f.close()
            return

        except IOError:
            self.send_error(404,'File Not Found: %s' % self.path)

    def do_POST(self):
        #if self.headers.getheader('Transfer-Encoding') == 'chunked':
        # todo
        #else:
        content_len = int(self.headers.getheader('Content-Length', 0))

        post_body = self.rfile.read(content_len)
        #print('post:', content_len, post_body)
        self.do_GET()
        return

try:
    server = myHTTPServer(('', args.port), myHandler)
    if args.https:
        os.system('openssl req -subj "/CN={host}" -new -newkey rsa:2048 -days 365 -nodes -x509 -keyout http_server.key -out http_server.crt'.format(host=args.host))
        server.socket = ssl.wrap_socket(server.socket, keyfile="http_server.key", certfile='http_server.crt', server_side=True)

    print 'Started http' + ( 's' if args.https else '' ) + ' server on port' , args.port
    server.serve_forever()

except KeyboardInterrupt:
    print '^C received, shutting down the web server'
    server.socket.close()
