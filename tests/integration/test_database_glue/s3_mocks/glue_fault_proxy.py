import http.client
import http.server
import json
import re
import socketserver
import sys
import threading

# A proxy in front of the moto Glue mock that fails `GetTables` for some databases.
# `GET /injected/<database>` returns how many faults were injected for that database.
UPSTREAM = ("glue", 3000)

# Database name prefix -> (HTTP status, exception name, message, number of requests to fail or None for all).
FAULTS = {
    "transient_": (500, "InternalServiceException", "Unknown exception occurred.", 2),
    "denied_": (400, "AccessDeniedException", "Injected access denied", None),
}

# moto ignores `UpdateTable.VersionId`, so the proxy enforces it like Glue does: an `UpdateTable` with a
# stale `VersionId` is refused with `ConcurrentModificationException`. `GET /updates/<database>` and
# `GET /rejected/<database>` return how many `UpdateTable` requests the proxy received and refused.
#
# Database name prefix -> a fault on the first `UpdateTable` for that database:
# - `touch_`: another writer changes a table parameter, but not the pointer, just before the request is checked;
# - `lostresp_`: the request is applied, but the client gets an HTTP 500 instead, so its SDK sends it again;
# - `superseded_`: like `lostresp_`, and another writer commits on top, recording the request's pointer as
#   `previous_metadata_location`;
# - `unknown_`: like `superseded_`, but the other writer does not record `previous_metadata_location`;
# - `readbackfail_`: like `lostresp_`, and the first `GetTable` after the refusal fails with a non-retryable error;
# - `lostfinal_`: like `lostresp_`, but the next `UpdateTable` (the SDK retry) is refused with `AccessDeniedException`
#   without being checked or applied;
# - `lostnoretry_`: the request is applied, but the client gets an HTTP 501, which its SDK does not retry;
# - `losttimeout_`: the request is applied, but the client gets `OperationTimeoutException` (HTTP 400), which its
#   SDK does not retry;
# - `failnoretry_`: the request is not applied and the client gets an HTTP 501;
# - `failtimeout_`: the request is not applied and the client gets `OperationTimeoutException` (HTTP 400);
# - `rejected_`: the request is not applied and the client gets `AccessDeniedException`;
# - `held_`: the request waits until `GET /release/<database>` (`GET /waiting/<database>` counts it), then is
#   checked and applied as usual.
UPDATE_FAULTS = (
    "touch_",
    "lostresp_",
    "superseded_",
    "unknown_",
    "readbackfail_",
    "lostfinal_",
    "lostnoretry_",
    "losttimeout_",
    "failnoretry_",
    "failtimeout_",
    "rejected_",
    "held_",
)

# Keys of a Glue `Table` that are also `TableInput` fields.
TABLE_INPUT_KEYS = (
    "Name",
    "Description",
    "Owner",
    "LastAccessTime",
    "LastAnalyzedTime",
    "Retention",
    "StorageDescriptor",
    "PartitionKeys",
    "ViewOriginalText",
    "ViewExpandedText",
    "TableType",
    "Parameters",
    "TargetTable",
)

# moto accepts any column text, Glue refuses a column name, comment or type that does not match `API_Column`.
GLUE_COLUMN_TEXT = re.compile(r"[\u0020-\ud7ff\ue000-\ufffd\U00010000-\U0010ffff\t]*")

DROPPED_HEADERS = {"connection", "keep-alive", "transfer-encoding", "content-length", "host", "server", "date"}

injected = {}
counters = {}
update_faults_taken = set()
# Databases whose read-back `GetTable` is armed to fail (`readbackfail_`, after the refusal).
readback_armed = set()
# Databases whose next `UpdateTable` is refused (`lostfinal_`, after the lost response).
deny_next_update = set()
# `GET /hold/<key>` answers only after `GET /release/<key>`; `GET /waiting/<key>` returns how many are held.
holds = {}
waiting = {}
lock = threading.Lock()
# Serializes the `VersionId` check with the update it guards.
update_lock = threading.Lock()


def take_fault(database):
    for prefix, (status, name, message, limit) in FAULTS.items():
        if not database.startswith(prefix):
            continue
        with lock:
            count = injected.get(database, 0)
            if limit is not None and count >= limit:
                return None
            injected[database] = count + 1
        return status, name, message
    return None


def take_update_fault(database):
    for prefix in UPDATE_FAULTS:
        if database.startswith(prefix):
            with lock:
                if database in update_faults_taken:
                    return None
                update_faults_taken.add(database)
            return prefix
    return None


def is_valid_column_text(text, max_length):
    return len(text.encode("utf-16-le")) // 2 <= max_length and GLUE_COLUMN_TEXT.fullmatch(text) is not None


def has_invalid_column(table_input):
    return any(
        not is_valid_column_text(column.get(key, ""), max_length)
        for column in table_input.get("StorageDescriptor", {}).get("Columns", [])
        for key, max_length in (("Name", 255), ("Comment", 255), ("Type", 131072))
    )


def increment(kind, database):
    with lock:
        counters[(kind, database)] = counters.get((kind, database), 0) + 1


def get_hold(key):
    with lock:
        return holds.setdefault(key, threading.Event())


def wait_for_release(key):
    event = get_hold(key)
    with lock:
        waiting[key] = waiting.get(key, 0) + 1
    try:
        event.wait(timeout=600)
    finally:
        with lock:
            waiting[key] -= 1


class RequestHandler(http.server.BaseHTTPRequestHandler):
    def reply(self, status, body, headers):
        self.send_response(status)
        for name, value in headers:
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def reply_glue_error(self, status, name, message):
        payload = json.dumps({"__type": name, "message": message}).encode()
        self.reply(status, payload, [("Content-Type", "application/x-amz-json-1.1")])

    def do_HEAD(self):
        self.send_response(200 if self.path.startswith("/hold/") else 404)
        self.send_header("Content-Type", "text/csv")
        self.send_header("Content-Length", "2" if self.path.startswith("/hold/") else "0")
        self.end_headers()

    def do_GET(self):
        if self.path == "/":
            self.reply(200, b"OK", [("Content-Type", "text/plain")])
        elif self.path.startswith("/injected/"):
            with lock:
                value = injected.get(self.path[len("/injected/") :], 0)
            self.reply(200, str(value).encode(), [("Content-Type", "text/plain")])
        elif self.path.startswith("/updates/") or self.path.startswith("/rejected/"):
            kind, database = self.path[1:].split("/", 1)
            with lock:
                value = counters.get((kind, database), 0)
            self.reply(200, str(value).encode(), [("Content-Type", "text/plain")])
        elif self.path.startswith("/hold/"):
            wait_for_release(self.path[len("/hold/") :])
            self.reply(200, b"2\n", [("Content-Type", "text/csv")])
        elif self.path.startswith("/release/"):
            get_hold(self.path[len("/release/") :]).set()
            self.reply(200, b"OK", [("Content-Type", "text/plain")])
        elif self.path.startswith("/waiting/"):
            with lock:
                value = waiting.get(self.path[len("/waiting/") :], 0)
            self.reply(200, str(value).encode(), [("Content-Type", "text/plain")])
        else:
            self.reply(*self.call_upstream(None))

    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        target = self.headers.get("X-Amz-Target")
        if target == "AWSGlue.GetTables":
            fault = take_fault(json.loads(body).get("DatabaseName", ""))
            if fault is not None:
                self.reply_glue_error(*fault)
                return
        elif target == "AWSGlue.GetTable":
            database = json.loads(body).get("DatabaseName", "")
            with lock:
                fail = database in readback_armed
                readback_armed.discard(database)
            if fail:
                self.reply_glue_error(400, "AccessDeniedException", "Injected read-back failure")
                return
        elif target == "AWSGlue.UpdateTable":
            self.update_table(body)
            return
        self.reply(*self.call_upstream(body))

    def update_table(self, body):
        request = json.loads(body)
        database = request.get("DatabaseName", "")
        name = request.get("TableInput", {}).get("Name", "")
        increment("updates", database)
        if has_invalid_column(request.get("TableInput", {})):
            self.reply_glue_error(400, "ValidationException", "Injected validation of a column name, comment or type.")
            return
        if database.startswith("held_") and take_update_fault(database) == "held_":
            wait_for_release(database)
        with update_lock:
            with lock:
                deny = database in deny_next_update
                deny_next_update.discard(database)
            if deny:
                self.reply_glue_error(400, "AccessDeniedException", "Injected access denied")
                return

            fault = take_update_fault(database)
            if fault == "failnoretry_":
                self.reply_glue_error(501, "InternalServiceException", "Injected failure.")
                return
            if fault == "failtimeout_":
                self.reply_glue_error(400, "OperationTimeoutException", "Injected timeout.")
                return
            if fault == "rejected_":
                self.reply_glue_error(400, "AccessDeniedException", "Injected access denied")
                return
            if fault == "touch_":
                table = self.get_table(database, name)
                table["Parameters"]["concurrent_touch"] = "1"
                self.put_table(database, table)

            if "VersionId" in request and request["VersionId"] != self.get_table_version(database, name):
                increment("rejected", database)
                if database.startswith("readbackfail_"):
                    with lock:
                        readback_armed.add(database)
                self.reply_glue_error(
                    400, "ConcurrentModificationException", "Update table failed due to concurrent modifications."
                )
                return

            status, content, headers = self.call_upstream(body)
            if status != 200 or fault in (None, "touch_"):
                self.reply(status, content, headers)
                return

            if fault in ("superseded_", "unknown_"):
                table = self.get_table(database, name)
                ours = table["Parameters"]["metadata_location"]
                table["Parameters"]["metadata_location"] = ours + "-superseded"
                if fault == "superseded_":
                    table["Parameters"]["previous_metadata_location"] = ours
                else:
                    table["Parameters"].pop("previous_metadata_location", None)
                self.put_table(database, table)
            elif fault == "lostfinal_":
                with lock:
                    deny_next_update.add(database)
            elif fault == "lostnoretry_":
                self.reply_glue_error(501, "InternalServiceException", "Injected lost response.")
                return
            elif fault == "losttimeout_":
                self.reply_glue_error(400, "OperationTimeoutException", "Injected timeout.")
                return
            self.reply_glue_error(500, "InternalServiceException", "Injected lost response.")

    def get_table_version(self, database, name):
        return self.call_glue("GetTable", {"DatabaseName": database, "Name": name})["Table"]["VersionId"]

    def get_table(self, database, name):
        table = self.call_glue("GetTable", {"DatabaseName": database, "Name": name})["Table"]
        return {key: table[key] for key in TABLE_INPUT_KEYS if key in table}

    def put_table(self, database, table_input):
        self.call_glue("UpdateTable", {"DatabaseName": database, "TableInput": table_input})

    def call_glue(self, operation, payload):
        status, content, _ = self.call_upstream(json.dumps(payload).encode(), operation)
        if status != 200:
            raise RuntimeError(f"Glue {operation} failed with {status}: {content}")
        # moto answers `UpdateTable` with an empty body.
        return json.loads(content) if content else {}

    def call_upstream(self, body, operation=None):
        headers = {k: v for k, v in self.headers.items() if k.lower() not in DROPPED_HEADERS}
        if operation is not None:
            headers["X-Amz-Target"] = f"AWSGlue.{operation}"
        connection = http.client.HTTPConnection(*UPSTREAM)
        try:
            connection.request(self.command, self.path, body=body, headers=headers)
            response = connection.getresponse()
            content = response.read()
        finally:
            connection.close()
        kept = [(k, v) for k, v in response.getheaders() if k.lower() not in DROPPED_HEADERS]
        return response.status, content, kept


class ThreadedHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True


ThreadedHTTPServer(("0.0.0.0", int(sys.argv[1])), RequestHandler).serve_forever()
