# -*- coding: utf-8 -*-
import argparse
import csv
import socket
import ssl
from http.server import BaseHTTPRequestHandler, HTTPServer


# Decorator used to see if authentication works for external dictionary who use a HTTP source.
def check_auth(fn):
    def wrapper(req):
        auth_header = req.headers.get("authorization", None)
        api_key = req.headers.get("api-key", None)
        if (
            not auth_header
            or auth_header != "Basic Zm9vOmJhcg=="
            or not api_key
            or api_key != "secret"
        ):
            req.send_response(401)
        else:
            fn(req)

    return wrapper


def start_server(server_address, data_path, schema, cert_path, address_family):
    class TSVHTTPHandler(BaseHTTPRequestHandler):
        @check_auth
        def do_GET(self):
            self.__send_headers()
            self.__send_data()

        @check_auth
        def do_POST(self):
            ids = self.__read_and_decode_post_ids()
            print("ids=", ids)
            self.__send_headers()
            self.__send_data(ids)

        def __send_headers(self):
            self.send_response(200)
            self.send_header("Content-type", "text/csv")
            self.end_headers()

        def __send_data(self, only_ids=None):
            with open(data_path, "r") as fl:
                reader = csv.reader(fl, delimiter="\t")
                for row in reader:
                    if not only_ids or (row[0] in only_ids):
                        self.wfile.write(("\t".join(row) + "\n").encode())

        def __read_and_decode_post_ids(self):
            data = self.__read_and_decode_post_data()
            return [_f for _f in data.split() if _f]

        def __read_and_decode_post_data(self):
            transfer_encoding = self.headers.get("Transfer-encoding")
            decoded = ""
            if transfer_encoding == "chunked":
                while True:
                    s = self.rfile.readline()
                    chunk_length = int(s, 16)
                    if not chunk_length:
                        break
                    decoded += self.rfile.read(chunk_length).decode()
                    self.rfile.readline()
            else:
                content_length = int(self.headers.get("Content-Length", 0))
                decoded = self.rfile.read(content_length).decode()
            return decoded

    if address_family == "ipv6":
        HTTPServer.address_family = socket.AF_INET6
    httpd = HTTPServer(server_address, TSVHTTPHandler)
    if schema == "https":
        httpd.socket = ssl.wrap_socket(
            httpd.socket, certfile=cert_path, server_side=True
        )
    httpd.serve_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Simple HTTP server returns data from file"
    )
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", default=5555, type=int)
    parser.add_argument("--data-path", required=True)
    parser.add_argument("--schema", choices=("http", "https"), required=True)
    parser.add_argument("--cert-path", default="./fake_cert.pem")
    parser.add_argument("--address-family", choices=("ipv4", "ipv6"), default="ipv4")

    args = parser.parse_args()

    start_server(
        (args.host, args.port),
        args.data_path,
        args.schema,
        args.cert_path,
        args.address_family,
    )
