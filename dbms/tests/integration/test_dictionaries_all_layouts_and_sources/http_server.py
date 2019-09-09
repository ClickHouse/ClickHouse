# -*- coding: utf-8 -*-
import argparse
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
import ssl
import csv
import os


def start_server(server_address, cert_path, data_path, schema):
    class TSVHTTPHandler(BaseHTTPRequestHandler):
        def _set_headers(self):
            self.send_response(200)
            self.send_header('Content-type', 'text/tsv')
            self.end_headers()

        def do_GET(self):
            self._set_headers()
            with open(data_path, 'r') as fl:
                reader = csv.reader(fl, delimiter='\t')
                for row in reader:
                    self.wfile.write('\t'.join(row) + '\n')
            return

        def do_POST(self):
            return self.do_GET()

    httpd = HTTPServer(server_address, TSVHTTPHandler)
    if schema == 'https':
        httpd.socket = ssl.wrap_socket(httpd.socket, certfile=cert_path, server_side=True)
    httpd.serve_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simple HTTP server returns data from file")
    parser.add_argument("--data-path", required=True)
    parser.add_argument("--schema", choices=("http", "https"), required=True)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", default=5555, type=int)
    parser.add_argument("--cert-path", default="./fake_cert.pem")

    args = parser.parse_args()

    start_server((args.host, args.port), args.cert_path, args.data_path, args.schema)
