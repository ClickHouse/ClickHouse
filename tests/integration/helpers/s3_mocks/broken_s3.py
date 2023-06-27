import logging
import sys
import threading
import random
import time
import urllib.parse
import http.server
import socketserver
import string


INF_COUNT = 100000000


class MockControl:
    def __init__(self, cluster, container, port):
        self._cluster = cluster
        self._container = container
        self._port = port

    def reset(self):
        response = self._cluster.exec_in_container(
            self._cluster.get_container_id(self._container),
            [
                "curl",
                "-s",
                f"http://localhost:{self._port}/mock_settings/reset",
            ],
            nothrow=True,
        )
        assert response == "OK", response

    def setup_error_at_object_upload(self, count=None, after=None):
        url = f"http://localhost:{self._port}/mock_settings/error_at_object_upload?nothing=1"

        if count is not None:
            url += f"&count={count}"

        if after is not None:
            url += f"&after={after}"

        response = self._cluster.exec_in_container(
            self._cluster.get_container_id(self._container),
            [
                "curl",
                "-s",
                url,
            ],
            nothrow=True,
        )
        assert response == "OK", response

    def setup_error_at_part_upload(self, count=None, after=None):
        url = f"http://localhost:{self._port}/mock_settings/error_at_part_upload?nothing=1"

        if count is not None:
            url += f"&count={count}"

        if after is not None:
            url += f"&after={after}"

        response = self._cluster.exec_in_container(
            self._cluster.get_container_id(self._container),
            [
                "curl",
                "-s",
                url,
            ],
            nothrow=True,
        )
        assert response == "OK", response

    def setup_error_at_create_multi_part_upload(self, count=None):
        url = f"http://localhost:{self._port}/mock_settings/error_at_create_multi_part_upload"

        if count is not None:
            url += f"?count={count}"

        response = self._cluster.exec_in_container(
            self._cluster.get_container_id(self._container),
            [
                "curl",
                "-s",
                url,
            ],
            nothrow=True,
        )
        assert response == "OK", response

    def setup_fake_puts(self, part_length):
        response = self._cluster.exec_in_container(
            self._cluster.get_container_id(self._container),
            [
                "curl",
                "-s",
                f"http://localhost:{self._port}/mock_settings/fake_puts?when_length_bigger={part_length}",
            ],
            nothrow=True,
        )
        assert response == "OK", response

    def setup_fake_multpartuploads(self):
        response = self._cluster.exec_in_container(
            self._cluster.get_container_id(self._container),
            [
                "curl",
                "-s",
                f"http://localhost:{self._port}/mock_settings/setup_fake_multpartuploads?",
            ],
            nothrow=True,
        )
        assert response == "OK", response

    def setup_slow_answers(
        self, minimal_length=0, timeout=None, probability=None, count=None
    ):
        url = (
            f"http://localhost:{self._port}/"
            f"mock_settings/slow_put"
            f"?minimal_length={minimal_length}"
        )

        if timeout is not None:
            url += f"&timeout={timeout}"

        if probability is not None:
            url += f"&probability={probability}"

        if count is not None:
            url += f"&count={count}"

        response = self._cluster.exec_in_container(
            self._cluster.get_container_id(self._container),
            ["curl", "-s", url],
            nothrow=True,
        )
        assert response == "OK", response


class _ServerRuntime:
    class SlowPut:
        def __init__(
            self, probability_=None, timeout_=None, minimal_length_=None, count_=None
        ):
            self.probability = probability_ if probability_ is not None else 1
            self.timeout = timeout_ if timeout_ is not None else 0.1
            self.minimal_length = minimal_length_ if minimal_length_ is not None else 0
            self.count = count_ if count_ is not None else INF_COUNT

        def __str__(self):
            return (
                f"probability:{self.probability}"
                f" timeout:{self.timeout}"
                f" minimal_length:{self.minimal_length}"
                f" count:{self.count}"
            )

        def get_timeout(self, content_length):
            if content_length > self.minimal_length:
                if self.count > 0:
                    if (
                        _runtime.slow_put.probability == 1
                        or random.random() <= _runtime.slow_put.probability
                    ):
                        self.count -= 1
                        return _runtime.slow_put.timeout
            return None

    class CountAfter:
        def __init__(self, count_=None, after_=None):
            self.count = count_ if count_ is not None else INF_COUNT
            self.after = after_ if after_ is not None else 0

        def __str__(self):
            return f"count:{self.count} after:{self.after}"

        def has_effect(self):
            if self.after:
                self.after -= 1
            if self.after == 0:
                if self.count:
                    self.count -= 1
                    return True
            return False

    def __init__(self):
        self.lock = threading.Lock()
        self.error_at_part_upload = None
        self.error_at_object_upload = None
        self.fake_put_when_length_bigger = None
        self.fake_uploads = dict()
        self.slow_put = None
        self.fake_multipart_upload = None
        self.error_at_create_multi_part_upload = None

    def register_fake_upload(self, upload_id, key):
        with self.lock:
            self.fake_uploads[upload_id] = key

    def is_fake_upload(self, upload_id, key):
        with self.lock:
            if upload_id in self.fake_uploads:
                return self.fake_uploads[upload_id] == key
        return False

    def reset(self):
        with self.lock:
            self.error_at_part_upload = None
            self.error_at_object_upload = None
            self.fake_put_when_length_bigger = None
            self.fake_uploads = dict()
            self.slow_put = None
            self.fake_multipart_upload = None
            self.error_at_create_multi_part_upload = None


_runtime = _ServerRuntime()


def _and_then(value, func):
    assert callable(func)
    return None if value is None else func(value)


def get_random_string(length):
    # choose from all lowercase letter
    letters = string.ascii_lowercase
    result_str = "".join(random.choice(letters) for i in range(length))
    return result_str


class RequestHandler(http.server.BaseHTTPRequestHandler):
    def _ok(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"OK")

    def _ping(self):
        self._ok()

    def _read_out(self):
        content_length = int(self.headers.get("Content-Length", 0))
        to_read = content_length
        while to_read > 0:
            # read content in order to avoid error on client
            # Poco::Exception. Code: 1000, e.code() = 32, I/O error: Broken pipe
            # do it piece by piece in order to avoid big allocation
            size = min(to_read, 1024)
            str(self.rfile.read(size))
            to_read -= size

    def _redirect(self):
        self._read_out()

        self.send_response(307)
        url = (
            f"http://{self.server.upstream_host}:{self.server.upstream_port}{self.path}"
        )
        self.send_header("Location", url)
        self.end_headers()
        self.wfile.write(b"Redirected")

    def _error(self, data):
        self._read_out()
        self.send_response(500)
        self.send_header("Content-Type", "text/xml")
        self.end_headers()
        self.wfile.write(bytes(data, "UTF-8"))

    def _error_expected_500(self):
        self._error(
            '<?xml version="1.0" encoding="UTF-8"?>'
            "<Error>"
            "<Code>ExpectedError</Code>"
            "<Message>mock s3 injected error</Message>"
            "<RequestId>txfbd566d03042474888193-00608d7537</RequestId>"
            "</Error>"
        )

    def _fake_put_ok(self):
        self._read_out()

        self.send_response(200)
        self.send_header("Content-Type", "text/xml")
        self.send_header("ETag", "b54357faf0632cce46e942fa68356b38")
        self.send_header("Content-Length", 0)
        self.end_headers()

    def _fake_uploads(self, path, upload_id):
        self._read_out()

        parts = [x for x in path.split("/") if x]
        bucket = parts[0]
        key = "/".join(parts[1:])
        data = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            "<InitiateMultipartUploadResult>\n"
            f"<Bucket>{bucket}</Bucket>"
            f"<Key>{key}</Key>"
            f"<UploadId>{upload_id}</UploadId>"
            "</InitiateMultipartUploadResult>"
        )

        self.send_response(200)
        self.send_header("Content-Type", "text/xml")
        self.send_header("Content-Length", len(data))
        self.end_headers()

        self.wfile.write(bytes(data, "UTF-8"))

    def _fake_post_ok(self, path):
        self._read_out()

        parts = [x for x in path.split("/") if x]
        bucket = parts[0]
        key = "/".join(parts[1:])
        location = "http://Example-Bucket.s3.Region.amazonaws.com/" + path
        data = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            "<CompleteMultipartUploadResult>\n"
            f"<Location>{location}</Location>\n"
            f"<Bucket>{bucket}</Bucket>\n"
            f"<Key>{key}</Key>\n"
            f'<ETag>"3858f62230ac3c915f300c664312c11f-9"</ETag>\n'
            f"</CompleteMultipartUploadResult>\n"
        )

        self.send_response(200)
        self.send_header("Content-Type", "text/xml")
        self.send_header("Content-Length", len(data))
        self.end_headers()

        self.wfile.write(bytes(data, "UTF-8"))

    def _mock_settings(self):
        parts = urllib.parse.urlsplit(self.path)
        path = [x for x in parts.path.split("/") if x]
        assert path[0] == "mock_settings", path
        if len(path) < 2:
            return self._error("_mock_settings: wrong command")

        if path[1] == "error_at_part_upload":
            params = urllib.parse.parse_qs(parts.query, keep_blank_values=False)
            _runtime.error_at_part_upload = _ServerRuntime.CountAfter(
                count_=_and_then(params.get("count", [None])[0], int),
                after_=_and_then(params.get("after", [None])[0], int),
            )
            return self._ok()

        if path[1] == "error_at_object_upload":
            params = urllib.parse.parse_qs(parts.query, keep_blank_values=False)
            _runtime.error_at_object_upload = _ServerRuntime.CountAfter(
                count_=_and_then(params.get("count", [None])[0], int),
                after_=_and_then(params.get("after", [None])[0], int),
            )
            return self._ok()

        if path[1] == "fake_puts":
            params = urllib.parse.parse_qs(parts.query, keep_blank_values=False)
            _runtime.fake_put_when_length_bigger = int(
                params.get("when_length_bigger", [1024 * 1024])[0]
            )
            return self._ok()

        if path[1] == "slow_put":
            params = urllib.parse.parse_qs(parts.query, keep_blank_values=False)
            _runtime.slow_put = _ServerRuntime.SlowPut(
                minimal_length_=_and_then(params.get("minimal_length", [None])[0], int),
                probability_=_and_then(params.get("probability", [None])[0], float),
                timeout_=_and_then(params.get("timeout", [None])[0], float),
                count_=_and_then(params.get("count", [None])[0], int),
            )
            self.log_message("set slow put %s", _runtime.slow_put)
            return self._ok()

        if path[1] == "setup_fake_multpartuploads":
            _runtime.fake_multipart_upload = True
            return self._ok()

        if path[1] == "error_at_create_multi_part_upload":
            params = urllib.parse.parse_qs(parts.query, keep_blank_values=False)
            _runtime.error_at_create_multi_part_upload = int(
                params.get("count", [INF_COUNT])[0]
            )
            return self._ok()

        if path[1] == "reset":
            _runtime.reset()
            return self._ok()

        return self._error("_mock_settings: wrong command")

    def do_GET(self):
        if self.path == "/":
            return self._ping()

        if self.path.startswith("/mock_settings"):
            return self._mock_settings()

        return self._redirect()

    def do_PUT(self):
        content_length = int(self.headers.get("Content-Length", 0))

        if _runtime.slow_put is not None:
            timeout = _runtime.slow_put.get_timeout(content_length)
            if timeout is not None:
                self.log_message("slow put %s", timeout)
                time.sleep(timeout)

        parts = urllib.parse.urlsplit(self.path)
        params = urllib.parse.parse_qs(parts.query, keep_blank_values=False)
        upload_id = params.get("uploadId", [None])[0]

        if upload_id is not None:
            if _runtime.error_at_part_upload is not None:
                if _runtime.error_at_part_upload.has_effect():
                    return self._error_expected_500()
            if _runtime.fake_multipart_upload:
                if _runtime.is_fake_upload(upload_id, parts.path):
                    return self._fake_put_ok()
        else:
            if _runtime.error_at_object_upload is not None:
                if _runtime.error_at_object_upload.has_effect():
                    return self._error_expected_500()
            if _runtime.fake_put_when_length_bigger is not None:
                if content_length > _runtime.fake_put_when_length_bigger:
                    return self._fake_put_ok()

        return self._redirect()

    def do_POST(self):
        parts = urllib.parse.urlsplit(self.path)
        params = urllib.parse.parse_qs(parts.query, keep_blank_values=True)
        uploads = params.get("uploads", [None])[0]
        if uploads is not None:
            if _runtime.error_at_create_multi_part_upload:
                _runtime.error_at_create_multi_part_upload -= 1
                return self._error_expected_500()

            if _runtime.fake_multipart_upload:
                upload_id = get_random_string(5)
                _runtime.register_fake_upload(upload_id, parts.path)
                return self._fake_uploads(parts.path, upload_id)

        upload_id = params.get("uploadId", [None])[0]
        if _runtime.is_fake_upload(upload_id, parts.path):
            return self._fake_post_ok(parts.path)

        return self._redirect()

    def do_HEAD(self):
        self._redirect()

    def do_DELETE(self):
        self._redirect()


class _ThreadedHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    """Handle requests in a separate thread."""

    def set_upstream(self, upstream_host, upstream_port):
        self.upstream_host = upstream_host
        self.upstream_port = upstream_port


if __name__ == "__main__":
    httpd = _ThreadedHTTPServer(("0.0.0.0", int(sys.argv[1])), RequestHandler)
    if len(sys.argv) == 4:
        httpd.set_upstream(sys.argv[2], sys.argv[3])
    else:
        httpd.set_upstream("minio1", 9001)
    httpd.serve_forever()
