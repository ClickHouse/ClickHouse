"""
Simple HTTP server that handles PUT requests
"""

from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import threading
from functools import partial
import logging


class DolorRequestHandler(BaseHTTPRequestHandler):
    def __init__(self, *args, callback=None, config=None, **kwargs):
        """
        Custom initializer for the request handler

        Args:
            callback: A callback function to process received data
            config: Configuration dictionary for the handler
        """
        self.callback = callback
        self.config = config if config is not None else {}
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
            if self.headers.get("Content-Type") == "application/json":
                data = json.loads(request_body.decode("utf-8"))
                if self.config.get("verbose", True):
                    self.logger.info(f"Parsed JSON: {data}")
            else:
                data = request_body.decode("utf-8")

            # Call the callback if provided
            response_ok = True
            if self.callback:
                response_ok = self.callback(self.path, data, self.headers)

            # Default response
            self.send_response(200 if response_ok else 400)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(
                json.dumps({"status": "OK" if response_ok else "Bad"}).encode("utf-8")
            )
        except json.JSONDecodeError as e:
            # Handle JSON parsing error
            self.logger.error(str(e))
            error_response = {"status": "error", "message": f"Invalid JSON: {str(e)}"}
            try:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(error_response).encode("utf-8"))
            except:
                pass
        except Exception as e:
            # Handle other errors
            self.logger.error(str(e))
            error_response = {"status": "error", "message": f"Server error: {str(e)}"}
            try:
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(error_response).encode("utf-8"))
            except:
                pass

    def log_message(self, format, *args):
        """Custom log format"""
        self.logger.info(f"{self.address_string()} - {format % args}")


class DolorHTTPServer:
    def __init__(self, host="localhost", port=8080, handler_kwargs=None):
        """
        Initialize the threaded HTTP server

        Args:
            host: Host to bind to
            port: Port to bind to
            handler_kwargs: Dictionary of keyword arguments to pass to the handler
                          (callback, config)
        """
        self.host = host
        self.port = port
        self.handler_kwargs = handler_kwargs if handler_kwargs else {}
        self.server = None
        self.thread = None
        self.is_running = False
        self.logger = logging.getLogger(__name__)

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
            self.logger.error(f"Server error: {e}")
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
        else:
            self.logger.error("Server is not running")

    def is_alive(self):
        """Check if the server thread is alive"""
        return self.thread is not None and self.thread.is_alive()
