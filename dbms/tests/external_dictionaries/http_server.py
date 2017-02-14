#!/usr/bin/env python
import os
import socket
import sys
import BaseHTTPServer
import SocketServer

PORT_NUMBER = 80
if len(sys.argv) > 1 and int(sys.argv[1]):
    PORT_NUMBER = int(sys.argv[1])

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
        self.do_GET()
        return

try:
    server = myHTTPServer(('', PORT_NUMBER), myHandler)
    print 'Started httpserver on port' , PORT_NUMBER
    server.serve_forever()

except KeyboardInterrupt:
    print '^C received, shutting down the web server'
    server.socket.close()
