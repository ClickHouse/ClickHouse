import ssl
from typing import Any


class WrapSSLContextWithSNI(ssl.SSLContext):
    def __new__(cls, ssl_host: str, *args: Any, **kwargs: Any) -> "WrapSSLContextWithSNI":  # pyright: ignore[reportAny, reportExplicitAny]
        self = super().__new__(cls, *args, **kwargs)
        self._server_hostname: str = ssl_host
        return self

    def wrap_socket(self, sock: Any, *args: Any, **kwargs: Any) -> Any:  # pyright: ignore[reportAny, reportExplicitAny]
        kwargs["server_hostname"] = self._server_hostname
        return super().wrap_socket(sock, *args, **kwargs)
