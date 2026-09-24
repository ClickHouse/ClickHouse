import logging
import signal


class PausedReadCancellation:
    partial_cancel_failpoint = "merge_tree_read_pool_pause_after_cancel"

    def __init__(
        self,
        node,
        query,
        query_id,
        read_failpoint,
        query_timeout=None,
        cancel_failpoint=partial_cancel_failpoint,
    ):
        self.node = node
        self.query = query
        self.query_id = query_id
        self.read_failpoint = read_failpoint
        self.query_timeout = query_timeout
        self.cancel_failpoint = cancel_failpoint
        self.query_request = None
        self.enabled_failpoints = []

    def __enter__(self):
        try:
            failpoints = (self.read_failpoint, self.cancel_failpoint) if self.cancel_failpoint else (self.read_failpoint,)
            for failpoint in failpoints:
                self.node.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
                self.enabled_failpoints.append(failpoint)

            options = {"query_id": self.query_id}
            if self.query_timeout is not None:
                options["timeout"] = self.query_timeout
            self.query_request = self.node.get_query_request(self.query, **options)
            self._wait(self.read_failpoint)
            return self
        except Exception:
            self._cleanup(preserve_error=True)
            raise

    def cancel(self):
        self.query_request.process.send_signal(signal.SIGINT)
        if self.cancel_failpoint:
            self._wait(self.cancel_failpoint)

    def resume(self):
        for failpoint in self.enabled_failpoints:
            self.node.query(f"SYSTEM NOTIFY FAILPOINT {failpoint}")

    def get_answer_and_error(self):
        return self.query_request.get_answer_and_error()

    def __exit__(self, exc_type, exc_value, traceback):
        self._cleanup(preserve_error=exc_type is not None)

    def _wait(self, failpoint):
        try:
            self.node.query(f"SYSTEM WAIT FAILPOINT {failpoint} PAUSE", timeout=60)
        except Exception as ex:
            raise RuntimeError(
                f"Query {self.query_id} did not pause at failpoint {failpoint}"
            ) from ex

    def _cleanup(self, preserve_error):
        cleanup_error = None

        def cleanup(action):
            nonlocal cleanup_error
            try:
                action()
            except Exception as ex:
                cleanup_error = cleanup_error or ex

        for failpoint in self.enabled_failpoints:
            cleanup(lambda failpoint=failpoint: self.node.query(f"SYSTEM NOTIFY FAILPOINT {failpoint}"))

        if self.query_request is not None and self.query_request.process.poll() is None:
            cleanup(self.query_request.process.kill)
            cleanup(self.query_request.process.wait)

        for failpoint in reversed(self.enabled_failpoints):
            cleanup(lambda failpoint=failpoint: self.node.query(f"SYSTEM DISABLE FAILPOINT {failpoint}"))
        self.enabled_failpoints.clear()

        if cleanup_error and preserve_error:
            logging.error("Cleanup failed for query %s: %s", self.query_id, cleanup_error)
        elif cleanup_error:
            raise cleanup_error
