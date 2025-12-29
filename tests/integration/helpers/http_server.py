# -*- coding: utf-8 -*-
import argparse
import csv
import json
import socket
import ssl
import logging
import signal
from http.server import BaseHTTPRequestHandler, HTTPServer


# Example OPA src JSONs
# {"input": {"context": {"identity": {"user": "default"}}, "access": {"database": "system", "table": "one", "columns": ["dummy"] }}}
# {"input": {"context": {"identity": {"user": "default"}}, "access": {"database": "system", "table": "numbers", "columns": ["number"] }}}
# {"input": {"context": {"identity": {"user": "default"}}, "access": {"database": "system", "table": "licenses", "columns": ["library_name", "license_type"] }}}
def opa_check_rules(src_json):
    try:
        return (
            src_json["input"]["context"]["identity"]["user"] != ""
            and src_json["input"]["access"]["database"] == "system"
            and src_json["input"]["access"]["table"] in ["one", "numbers", "licenses"]
            and all(not col.startswith("license") for col in src_json["input"]["access"]["columns"])
        )
    except Exception as e:
        print(type(e).__name__, e)
        return None


def opa_result_json(allow):
    if allow:
        return """{ "result": true }"""
    else:
        return """{}"""


def check_header_auth(headers, token):
    auth_header = headers.get("authorization", None)
    return auth_header and (auth_header == ("Basic " + token) or auth_header == ("Bearer " + token))


def check_header_api_key(headers, require_api_key):
    if not require_api_key:
        return True
    api_key = headers.get("api-key", None)
    return api_key and api_key == "secret"


# Decorator used to see if authentication works for external dictionary who use a HTTP source.
def check_auth(token, require_api_key):
    def check_auth_impl(fn):
        def wrapper(req):
            if (
                not check_header_auth(req.headers, token)
                or not check_header_api_key(req.headers, require_api_key)
            ):
                req.send_response(401)
                req.send_header("WWW-Authenticate", "Basic")
                req.send_header("WWW-Authenticate", "Bearer")
                req.end_headers()
            else:
                fn(req)

        return wrapper
    return check_auth_impl


def start_server(server_address, data_path, schema, cert_path, address_family, token, is_opa):
    class TSVHTTPHandler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            logging.info(
                "%s:%s - - [%s] %s",
                self.address_string(),
                self.server.server_port,
                self.log_date_time_string(),
                format % args,
            )

        @check_auth(token, require_api_key=(not is_opa))
        def do_GET(self):
            self.log_message("Processing '%s'", self.requestline)
            if is_opa:
                dst_json_str = opa_result_json(False)
                self.__send_headers("application/json")
                self.__send_string(dst_json_str)
            else:
                self.__send_headers()
                self.__send_data()

        @check_auth(token, require_api_key=(not is_opa))
        def do_POST(self):
            self.log_message("Processing '%s'", self.requestline)
            if is_opa:
                src_json = self.__read_json()
                self.log_message("src json '%s'", json.dumps(src_json, indent=2))
                allow = opa_check_rules(src_json)
                dst_json_str = opa_result_json(allow)
                self.log_message("dst json '%s'", dst_json_str)
                self.__send_headers("application/json")
                self.__send_string(dst_json_str)
            else:
                ids = self.__read_and_decode_post_ids()
                print("ids=", ids)
                self.__send_headers()
                self.__send_data(ids)

        def __send_headers(self, content_type="text/tsv"):
            self.send_response(200)
            self.send_header("Content-type", content_type)
            self.end_headers()

        def __send_data(self, only_ids=None):
            with open(data_path, "r") as fl:
                reader = csv.reader(fl, delimiter="\t")
                for row in reader:
                    if not only_ids or (row[0] in only_ids):
                        self.wfile.write(("\t".join(row) + "\n").encode())

        def __send_string(self, result_str):
            self.wfile.write(result_str.encode())

        def __read_and_decode_post_ids(self):
            data = self.__read_and_decode_post_data()
            return [_f for _f in data.split() if _f]

        def __read_and_decode_post_data(self):
            transfer_encoding = self.headers.get("Transfer-encoding")
            decoded = ""
            if transfer_encoding == "chunked":
                while True:
                    s = self.rfile.readline().decode()
                    chunk_length = int(s, 16)
                    if not chunk_length:
                        break
                    decoded += self.rfile.read(chunk_length).decode()
                    self.rfile.readline().decode()
            else:
                content_length = int(self.headers.get("Content-Length", 0))
                decoded = self.rfile.read(content_length).decode()
            return decoded

        def __read_json(self):
            try:
                content_length = int(self.headers.get("Content-Length", 0))
                json_data = self.rfile.read(content_length)
                return json.loads(json_data)
            except json.JSONDecodeError as e:
                print(type(e).__name__, e)
                return None

    if address_family == "ipv6":
        HTTPServer.address_family = socket.AF_INET6
    httpd = HTTPServer(server_address, TSVHTTPHandler)
    if schema == "https":
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        #ssl_context.load_cert_chain(cert_path)
        ssl_context.load_verify_locations(cert_path)
        httpd.socket = ssl_context.wrap_socket(httpd.socket, server_side=True)
    logging.info("Starting serving on %s", httpd.server_address)
    httpd.serve_forever()
    # Never reaches here


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.NOTSET,
        format="%(process)d %(asctime)s %(levelname)s: %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Simple HTTP server returns data from file"
    )
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", default=5555, type=int)
    parser.add_argument("--data-path", required=True)
    parser.add_argument("--schema", choices=("http", "https"), required=True)
    parser.add_argument("--cert-path", default="./fake_cert.pem")
    parser.add_argument("--address-family", choices=("ipv4", "ipv6"), default="ipv4")
    parser.add_argument("--token", default="Zm9vOmJhcg==")
    parser.add_argument("--mode", choices=("ids", "opa"), default="ids")

    args = parser.parse_args()

    start_server(
        (args.host, args.port),
        args.data_path,
        args.schema,
        args.cert_path,
        args.address_family,
        args.token,
        args.mode == "opa"
    )
