import ssl


class WrapSSLContextWithSNI(ssl.SSLContext):
    def __new__(cls, ssl_host, *args, **kwargs):
        self = super().__new__(cls, *args, **kwargs)
        self._server_hostname = ssl_host
        return self

    def wrap_socket(self, sock, *args, **kwargs):
        kwargs["server_hostname"] = self._server_hostname
        return super().wrap_socket(sock, *args, **kwargs)
