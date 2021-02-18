import BaseHTTPServer

RESULT_PATH = '/result.txt'

class SentryHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    def do_POST(self):
        post_data = self.__read_and_decode_post_data()
        with open(RESULT_PATH, 'w') as f:
            if self.headers.get("content-type") != "application/x-sentry-envelope":
                f.write("INCORRECT_CONTENT_TYPE")
            elif self.headers.get("content-length") < 3000:
                f.write("INCORRECT_CONTENT_LENGTH")
            elif '"http://6f33034cfe684dd7a3ab9875e57b1c8d@localhost:9500/5226277"' not in post_data:
                f.write('INCORRECT_POST_DATA')
            else:
                f.write("OK")
        self.send_response(200)

    def __read_and_decode_post_data(self):
        transfer_encoding = self.headers.get("transfer-Encoding")
        decoded = ""
        if transfer_encoding == "chunked":
            while True:
                s = self.rfile.readline()
                chunk_length = int(s, 16)
                if not chunk_length:
                    break
                decoded += self.rfile.read(chunk_length)
                self.rfile.readline()
        else:
            content_length = int(self.headers.get("content-length", 0))
            decoded = self.rfile.read(content_length)
        return decoded


if __name__ == "__main__":
    with open(RESULT_PATH, 'w') as f:
        f.write("INITIAL_STATE")
    httpd = BaseHTTPServer.HTTPServer(("localhost", 9500,), SentryHandler)
    try:
        httpd.serve_forever()
    finally:
        httpd.server_close()
