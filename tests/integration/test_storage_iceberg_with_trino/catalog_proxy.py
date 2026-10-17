import json
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import requests


@contextmanager
def catalog_commit_barrier(catalog_port):
    """Forward REST requests and allow a test to pause one table's snapshot commit."""
    paused = threading.Event()
    resume = threading.Event()
    table_to_pause = None

    class CatalogProxy(BaseHTTPRequestHandler):
        def forward(self):
            body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            if (
                table_to_pause is not None
                and self.command == "POST"
                and self.path.endswith(f"/tables/{table_to_pause}")
                and any(
                    update.get("action") == "add-snapshot"
                    for update in json.loads(body).get("updates", [])
                )
                and not paused.is_set()
            ):
                paused.set()
                assert resume.wait(120), "Timed out waiting for the ClickHouse drop"
            response = requests.request(
                self.command,
                f"http://127.0.0.1:{catalog_port}{self.path}",
                data=body,
                headers={"Content-Type": "application/json"},
                timeout=30,
            )
            self.send_response(response.status_code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(response.content)))
            self.end_headers()
            self.wfile.write(response.content)

        do_GET = forward
        do_POST = forward

        def log_message(self, *_):
            pass

    @contextmanager
    def pause_commit(table_name):
        nonlocal table_to_pause
        paused.clear()
        resume.clear()
        table_to_pause = table_name
        try:
            yield paused
        finally:
            resume.set()
            table_to_pause = None

    proxy = ThreadingHTTPServer(("0.0.0.0", 0), CatalogProxy)
    thread = threading.Thread(target=proxy.serve_forever, daemon=True)
    thread.start()
    try:
        yield proxy.server_port, pause_commit
    finally:
        resume.set()
        proxy.shutdown()
        proxy.server_close()
        thread.join(timeout=10)
