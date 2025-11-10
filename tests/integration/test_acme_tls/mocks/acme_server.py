import http.server
import ssl
import sys
import threading
import os

from certificates import generate_self_signed_cert
from handler import RequestHandler

HOSTNAME = f"{os.uname().nodename}:8443"


if __name__ == "__main__":
    private_key_pem, cert_pem = generate_self_signed_cert(HOSTNAME)

    with open("key.pem", "wb") as key_file:
        key_file.write(private_key_pem)
    with open("cert.pem", "wb") as cert_file:
        cert_file.write(cert_pem)

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain("cert.pem", "key.pem")

    RequestHandler.HOSTNAME = HOSTNAME

    httpd = http.server.HTTPServer(("0.0.0.0", int(sys.argv[1])), RequestHandler)
    httpd_secure = http.server.ThreadingHTTPServer(("0.0.0.0", 8443), RequestHandler)
    httpd_secure.socket = context.wrap_socket(
        httpd_secure.socket,
        server_side=True,
    )

    t1 = threading.Thread(target=httpd.serve_forever)
    t2 = threading.Thread(target=httpd_secure.serve_forever)

    for t in [t1, t2]:
        t.start()

    for t in [t1, t2]:
        t.join()
