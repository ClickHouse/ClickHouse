"""
Simple HTTP server that handles PUT requests with async support
"""

from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import threading
from functools import partial
from concurrent.futures import ThreadPoolExecutor
import logging


class BackgroundWorker:
    """Background worker using ThreadPoolExecutor"""

    def __init__(self, max_workers=4):
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.logger = logging.getLogger(__name__)

    def enqueue(self, func, *args, **kwargs):
        """Submit a task to the thread pool"""
        future = self.executor.submit(func, *args, **kwargs)
        # Optionally add a callback for errors
        future.add_done_callback(self._handle_result)
        return future

    def _handle_result(self, future):
        """Handle task completion or errors"""
        try:
            result = future.result()  # This will raise if the task failed
            self.logger.debug(f"Async task completed successfully")
        except Exception as e:
            self.logger.exception(f"Async task failed: {e}")

    def shutdown(self, wait=True):
        """Shutdown the thread pool"""
        self.executor.shutdown(wait=wait)
        self.logger.info("Background worker stopped")


class DolorRequestHandler(BaseHTTPRequestHandler):
    def __init__(
        self, *args, callback=None, attachment=None, config=None, worker=None, **kwargs
    ):
        """
        Custom initializer for the request handler

        Args:
            callback: A callback function to process received data
            attachment: Attachment object passed to callback
            config: Configuration dictionary for the handler
            worker: BackgroundWorker instance for async requests
        """
        self.callback = callback
        self.attachment = attachment
        self.config = config if config is not None else {}
        self.worker = worker
        self.logger = logging.getLogger(__name__)

        # Call the parent class initializer
        super().__init__(*args, **kwargs)

    def do_PUT(self):
        # Get the content length
        content_length = int(self.headers.get("Content-Length", 0))
        # Read the request body
        request_body = self.rfile.read(content_length)

        # Log the request
        if self.config.get("verbose", False):
            self.logger.info(f"PUT request received at: {self.path}")
            self.logger.info(f"Headers: {self.headers}")
            self.logger.info(f"Body: {request_body.decode('utf-8')}")

        # Process the request
        try:
            # Parse JSON if applicable
            data = None
            is_async = False

            if self.headers.get("Content-Type") == "application/json":
                data = json.loads(request_body.decode("utf-8"))
                # Check if this is an async request
                is_async = data.get("async", 0) == 1
                if self.config.get("verbose", True):
                    self.logger.info(f"Parsed JSON: {data}")
            else:
                data = request_body.decode("utf-8")

            # Handle async vs sync requests
            if is_async and self.worker:
                # Process asynchronously
                self.worker.enqueue(
                    self._process_callback,
                    self.path,
                    data,
                    dict(self.headers),  # Convert headers to dict for thread safety
                    self.attachment,
                )

                # Send immediate response
                self.send_response(200)  # 200 Ok
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(
                    json.dumps(
                        {"status": "accepted", "message": "Processing asynchronously"}
                    ).encode("utf-8")
                )
            else:
                # Process synchronously
                response_ok = self._process_callback(
                    self.path, data, self.headers, self.attachment
                )

                # Send response
                self.send_response(200 if response_ok else 400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(
                    json.dumps({"status": "OK" if response_ok else "Bad"}).encode(
                        "utf-8"
                    )
                )

        except json.JSONDecodeError as e:
            # Handle JSON parsing error
            self.logger.exception(e)
            self._send_error_response(400, f"Invalid JSON: {str(e)}")
        except BrokenPipeError:
            self.logger.debug(
                f"Client disconnected before response was sent: {self.path}"
            )
        except Exception as e:
            # Handle other errors
            self.logger.exception(e)
            self._send_error_response(500, f"Server error: {str(e)}")

    def _process_callback(self, path, data, headers, attachment):
        """Process the callback - can be called sync or async"""
        try:
            if self.callback:
                return self.callback(path, data, headers, attachment)
            return True
        except Exception as e:
            self.logger.exception(f"Callback error: {e}")
            return False

    def _send_error_response(self, status_code, message):
        """Send an error response"""
        error_response = {"status": "error", "message": message}
        try:
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(error_response).encode("utf-8"))
        except:
            pass

    def log_message(self, format, *args):
        """Custom log format"""
        self.logger.info(f"{self.address_string()} - {format % args}")


class DolorHTTPServer:
    def __init__(
        self,
        host="localhost",
        port=8080,
        attachment=None,
        handler_kwargs={},
        max_workers=16,
    ):
        """
        Initialize the threaded HTTP server

        Args:
            host: Host to bind to
            port: Port to bind to
            attachment: Attachment object to pass to handlers
            handler_kwargs: Dictionary of keyword arguments to pass to the handler
                          (callback, config)
            max_workers: Maximum number of worker threads for async requests
        """
        self.host = host
        self.port = port
        self.handler_kwargs = handler_kwargs
        self.server = None
        self.thread = None
        self.is_running = False
        self.logger = logging.getLogger(__name__)
        self.attachment = attachment
        # Create background worker
        self.worker = BackgroundWorker(max_workers=max_workers)
        # Add worker and attachment to handler kwargs
        self.handler_kwargs["attachment"] = self.attachment
        self.handler_kwargs["worker"] = self.worker

    def start(self):
        """Start the server in a separate thread"""
        if self.is_running:
            self.logger.error("Server is already running")
            return

        # Create a handler factory with custom arguments
        handler_factory = partial(DolorRequestHandler, **self.handler_kwargs)

        self.server = HTTPServer((self.host, self.port), handler_factory)
        self.thread = threading.Thread(target=self._run_server, daemon=True)
        self.thread.start()
        self.is_running = True
        self.logger.info(
            f"HTTP server started on http://{self.host}:{self.port} in background thread"
        )

    def _run_server(self):
        """Internal method to run the server"""
        try:
            self.server.serve_forever()
        except Exception as e:
            self.logger.exception(f"Server error: {e}")
        finally:
            self.is_running = False

    def stop(self):
        """Stop the server"""
        if self.server and self.is_running:
            self.server.shutdown()
            self.server.server_close()
            self.thread.join(timeout=5)
            self.is_running = False
            self.logger.info("Server stopped")
            # Shutdown the worker pool
            self.worker.shutdown(wait=True)
        else:
            self.logger.error("Server is not running")

    def is_alive(self):
        """Check if the server thread is alive"""
        return self.thread is not None and self.thread.is_alive()
