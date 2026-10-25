#!/usr/bin/env python3
"""Run the queries of a `.sql` test, read from stdin, over a protocol other than the native one.

`clickhouse-test --protocols` uses it to run every `.sql` test over a random protocol, so that the
same randomized and fuzzed workload that the Stress test sends over the native protocol reaches the
other protocol handlers too. The output is not expected to match the `.reference` files: every
protocol formats results its own way. What matters is how the server behaves.

Like `clickhouse-client` in a `.sql` test, it runs the queries one by one in one session and stops
at the first error, unless the query carries a `-- { serverError ... }`, `-- { clientError ... }` or
`-- { error ... }` hint with the code of the error. It also stops when a query with such a hint
succeeds. The `ssh` and `webterminal` protocols run the embedded client inside the server, which parses
the queries and applies the hints itself.

The non-standard Python modules (`grpc`, `pyarrow`) are imported only by the protocols that need them.
"""

import argparse
import base64
import hashlib
import http.client
import json
import os
import re
import shutil
import signal
import socket
import ssl
import struct
import subprocess
import sys
import tempfile
import urllib.parse
import uuid

PROTOCOLS = [
    "http",
    "https",
    "mysql",
    "postgresql",
    "grpc",
    "arrowflight",
    "ssh",
    "webterminal",
]

# The server-side end of a query is governed by the test timeout of `clickhouse-test`, which kills
# this process. These timeouts only bound waiting on a peer that went silent.
SOCKET_TIMEOUT = 600

# `ssh` passes the whole test as the command of the channel, which travels in one `argv` element of
# the `ssh` client; Linux limits one element to 128 KiB (`MAX_ARG_STRLEN`).
SSH_MAX_QUERY_SIZE = 100 * 1024
# A test of at most this size leaves enough room in `SSH_MAX_QUERY_SIZE` for the setup queries.
SSH_MAX_TEST_SIZE = 90 * 1024

# The protocols that carry arbitrary bytes of a query: a NUL byte or invalid UTF-8 in it. The others
# need a valid string: gRPC and Arrow Flight send it in a protobuf `string`, PostgreSQL terminates it
# with a NUL byte, SSH passes it in an `argv` element, and the web terminal pastes it into a line editor.
BINARY_PROTOCOLS = {"http", "https", "mysql"}


class QueryError(Exception):
    def __init__(self, message, code=None):
        super().__init__(message)
        # The ClickHouse error code, if the protocol reports it; otherwise it is taken from the
        # `Code: N.` prefix of the message, if there is one.
        if code is None and (match := CODE_RE.search(message)):
            code = int(match.group(1))
        self.code = code


CODE_RE = re.compile(r"\bCode: (\d+)\.")


def env_port(name, default):
    return int(os.environ.get(name) or default)


def sql_literal(value):
    """Quote the value of a setting or a query parameter for `SET name = value`. Like
    `clickhouse-client` does with a value from its command line, the server parses the string."""
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def parse_key_values(items):
    result = []
    for item in items:
        key, sep, value = item.partition("=")
        if not sep:
            raise ValueError(f"Expected name=value, got {item!r}")
        result.append((key, value))
    return result


# Splitting the test into queries.
#
# This mirrors what `ClientBase` does in a multi-query: the queries are separated by top-level
# semicolons, while string literals, quoted identifiers, comments and heredocs may contain anything.
# The data of `INSERT ... FORMAT <not Values>` lasts until an empty line, as in `ClientBase`.

HINT_RE = re.compile(r"\{\s*(serverError|clientError|error)\b")
# The list of the error codes or names of the hint, separated by commas, as `TestHint` parses it.
ERROR_HINT_RE = re.compile(r"\{\s*(?:serverError|clientError|error)\b([^}]*)\}")
# `ParserInsertQuery` also accepts a `WITH` clause before `INSERT`.
INSERT_RE = re.compile(r"\s*(EXPLAIN|INSERT|WITH)\b", re.IGNORECASE)
FORMAT_RE = re.compile(r"FORMAT\s+(\w+)\b", re.IGNORECASE)
HEREDOC_RE = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)?\$")


def skip_quoted(text, pos, quote):
    """Return the position after the literal that starts at `pos` with `quote`."""
    pos += 1
    while pos < len(text):
        char = text[pos]
        if char == "\\":
            pos += 2
            continue
        if char == quote:
            if pos + 1 < len(text) and text[pos + 1] == quote:
                pos += 2
                continue
            return pos + 1
        pos += 1
    return pos


def skip_block_comment(text, pos):
    depth = 0
    while pos < len(text):
        if text.startswith("/*", pos):
            depth += 1
            pos += 2
        elif text.startswith("*/", pos):
            depth -= 1
            pos += 2
            if depth == 0:
                return pos
        else:
            pos += 1
    return pos


def is_line_comment(text, pos):
    if text.startswith("--", pos):
        return True
    # `#` starts a comment only when followed by a space or `!`, as in the ClickHouse lexer.
    return text[pos] == "#" and (pos + 1 == len(text) or text[pos + 1] in " !")


def split_queries(text):
    """Split the text into `(query, hint)` pairs. The hint is the comment that follows the query on
    the line of its terminating semicolon, as in `ClientBase`."""
    queries = []
    begin = 0
    pos = 0
    has_tokens = False
    while pos < len(text):
        char = text[pos]
        if char in "'\"`":
            pos = skip_quoted(text, pos, char)
            has_tokens = True
        elif text.startswith("/*", pos):
            pos = skip_block_comment(text, pos)
        elif is_line_comment(text, pos):
            end = text.find("\n", pos)
            pos = len(text) if end == -1 else end
        elif char == "$" and (heredoc := HEREDOC_RE.match(text, pos)):
            end = text.find(heredoc.group(0), heredoc.end())
            pos = len(text) if end == -1 else end + len(heredoc.group(0))
            has_tokens = True
        elif char == ";":
            pos = finish_query(text, begin, pos, queries, has_tokens)
            begin = pos
            has_tokens = False
        elif (insert_format := match_insert_format(text, begin, pos)) is not None:
            # The data of an `INSERT ... FORMAT <not Values>` starts right after the format name, on
            # the same line or the next ones, and ends at an empty line, regardless of semicolons in it.
            end = text.find("\n\n", insert_format)
            end = len(text) if end == -1 else end
            # Only the SQL before the data is trimmed: whitespace at the end of the data can be a
            # field separator or a part of a value.
            queries.append((text[begin:end].lstrip(), ""))
            begin = pos = end
            has_tokens = False
        else:
            if not char.isspace():
                has_tokens = True
            pos += 1
    if has_tokens:
        queries.append((text[begin:].strip(), ""))
    return queries


def match_insert_format(text, begin, pos):
    """If the `FORMAT <name>` of an `INSERT` with inline data in a format other than `Values` starts at
    `pos`, return the position after the format name, like `ParserInsertQuery`, otherwise `None`."""
    if pos > 0 and (text[pos - 1].isalnum() or text[pos - 1] == "_"):
        return None
    insert_format = FORMAT_RE.match(text, pos)
    if not insert_format or insert_format.group(1) == "Values":
        return None
    # A `;` right after the format name is a syntax error the server reports.
    if text.startswith(";", insert_format.end()):
        return None
    statement = text[begin:pos]
    if not INSERT_RE.match(strip_comments(statement)):
        return None
    # Only the format of an `INSERT` (possibly explained) starts the data, not the format of a nested
    # query, like in `EXPLAIN AST (SELECT 1 FORMAT TSV)` or `EXPLAIN SELECT 1 FORMAT TSV`.
    words = set(top_level_words(statement))
    if "INSERT" not in words or not is_top_level_end(statement):
        return None
    # The format of `WITH ... SELECT ... FORMAT TSV` does not start any data.
    if next(top_level_words(statement), None) == "WITH" and returns_result(statement):
        return None
    # The rows of `INSERT ... FROM INFILE` and of `INSERT ... SELECT` do not follow the format name,
    # unless the `SELECT` reads them with the `input` table function.
    if "INFILE" in words or ("SELECT" in words and not reads_input(statement)):
        return None
    return insert_format.end()


def finish_query(text, begin, pos, queries, has_tokens):
    """Record the query that ends with the semicolon at `pos`, return where the next one begins."""
    query = text[begin:pos].strip()
    end = pos + 1
    hint_end = trailing_hint_end(text, end)
    if hint_end is not None and HINT_RE.search(text, end, hint_end):
        hint = text[end:hint_end]
        end = hint_end
    else:
        hint = ""
    if has_tokens:
        queries.append((query, hint))
    return end


def trailing_hint_end(text, pos):
    """Like `ClientBase::adjustQueryEnd`: the rest of the line after the semicolon, together with
    more semicolons, belongs to the query if no next query starts on this line. Return its end, or
    `None` if the next query starts on this line, like in `SELECT 1; SELECT throwIf(1); -- { serverError
    FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }`, where the hint belongs to the second query."""
    newline = text.find("\n", pos)
    line_end = len(text) if newline == -1 else newline
    while pos < len(text):
        if text[pos].isspace() or text[pos] == ";":
            pos += 1
        elif text.startswith("/*", pos):
            comment_end = skip_block_comment(text, pos)
            # A comment that continues on the next lines belongs to the query as a whole.
            if pos < line_end < comment_end:
                line_end = comment_end
            pos = comment_end
        elif is_line_comment(text, pos):
            newline = text.find("\n", pos)
            pos = len(text) if newline == -1 else newline
        else:
            break
    return line_end if pos >= line_end else None


def is_top_level_end(statement):
    """Whether the end of the statement is outside of parentheses."""
    query = strip_comments(statement)
    depth = 0
    pos = 0
    while pos < len(query):
        char = query[pos]
        if char in "'\"`":
            pos = skip_quoted(query, pos, char)
            continue
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        pos += 1
    return depth == 0


def strip_comments(query):
    """Remove the comments from the query; the line structure of the remaining text is kept."""
    result = []
    pos = 0
    while pos < len(query):
        char = query[pos]
        if char in "'\"`":
            end = skip_quoted(query, pos, char)
            result.append(query[pos:end])
            pos = end
        elif query.startswith("/*", pos):
            pos = skip_block_comment(query, pos)
            result.append(" ")
        elif is_line_comment(query, pos):
            end = query.find("\n", pos)
            pos = len(query) if end == -1 else end
        else:
            result.append(char)
            pos += 1
    return "".join(result).strip()


def iter_comments(query):
    """Yield the comments of the query, each with whether it precedes the first token."""
    leading = True
    pos = 0
    while pos < len(query):
        char = query[pos]
        if char in "'\"`":
            pos = skip_quoted(query, pos, char)
            leading = False
        elif query.startswith("/*", pos):
            end = skip_block_comment(query, pos)
            yield query[pos:end], leading
            pos = end
        elif is_line_comment(query, pos):
            end = query.find("\n", pos)
            end = len(query) if end == -1 else end
            yield query[pos:end], leading
            pos = end
        else:
            if not char.isspace():
                leading = False
            pos += 1


def expected_errors(query, hint):
    """The error codes and names that the hint of the query expects, or `None` if the query is
    expected to succeed. Like `TestHint`, it takes the hints from the comments in the query and
    after it, except the leading comments, and the last hint wins.

    The protocols have no client side that could fail by itself, so a `clientError` hint is
    satisfied by a server error with one of its codes, the same as `serverError`: what the native
    client rejects before sending, like a syntax error, the server rejects with the same code."""
    comments = list(iter_comments(query)) + [(comment, False) for comment, _ in iter_comments(hint)]
    expected = None
    for comment, leading in comments:
        if leading:
            continue
        if match := ERROR_HINT_RE.search(comment):
            expected = [item.strip() for item in match.group(1).split(",") if item.strip()]
    return expected


class ErrorCodes:
    """The codes of the errors by their names, from `system.errors` of the server, loaded on the
    first use. It is queried over HTTP regardless of the protocol of the test."""

    def __init__(self, host):
        self.host = host
        self.codes = None

    def resolve(self, item):
        if item.isdigit():
            return int(item)
        if self.codes is None:
            connection = http.client.HTTPConnection(
                self.host, env_port("CLICKHOUSE_PORT_HTTP", 8123), timeout=SOCKET_TIMEOUT
            )
            query = "SELECT name, code FROM system.errors SETTINGS system_events_show_zero_values = 1"
            connection.request("POST", "/", body=query.encode())
            response = connection.getresponse()
            data = response.read().decode()
            connection.close()
            if response.status != 200:
                raise QueryError(f"Cannot load the error codes from system.errors: {data}")
            self.codes = {name: int(code) for name, code in (line.split("\t") for line in data.splitlines())}
        if item not in self.codes:
            raise QueryError(f"Unknown error name {item!r} in the test hint")
        return self.codes[item]


def check_error(client, error, expected, error_codes):
    """Return the description of the mismatch of the error with the hint, or `None` if it matches."""
    codes = [error_codes.resolve(item) for item in expected]
    if error.code is None and not client.reports_error_codes:
        # Nothing to compare with: the protocol did not report the code of this error.
        return None
    if error.code is None:
        return f"Expected an error with the code in {expected}, got an error without a code: {error}"
    if error.code not in codes:
        return f"Expected an error with the code in {expected}, got the code {error.code}: {error}"
    return None


RESULT_KEYWORDS = {"SELECT", "WITH", "SHOW", "DESC", "DESCRIBE", "EXPLAIN", "EXISTS", "CHECK"}


WORD_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def top_level_words(query, any_depth=False):
    """Yield the upper-cased words of the query outside of literals, comments and, unless
    `any_depth`, parentheses."""
    query = strip_comments(query)
    depth = 0
    pos = 0
    while pos < len(query):
        char = query[pos]
        if char in "'\"`":
            pos = skip_quoted(query, pos, char)
        elif char == "(":
            depth += 1
            pos += 1
        elif char == ")":
            depth -= 1
            pos += 1
        elif word := WORD_RE.match(query, pos):
            if depth == 0 or any_depth:
                yield word.group(0).upper()
            pos = word.end()
        else:
            pos += 1


INPUT_FUNCTION_RE = re.compile(r"\binput\s*\(", re.IGNORECASE)


def reads_input(statement):
    """Whether the statement calls the `input` table function, which reads the inline data."""
    if "INPUT" not in top_level_words(statement, any_depth=True):
        return False
    # Literals and quoted identifiers become empty, so that only an actual call matches.
    text = strip_comments(statement)
    unquoted = []
    pos = 0
    while pos < len(text):
        char = text[pos]
        if char in "'\"`":
            end = skip_quoted(text, pos, char)
            unquoted.append(char + char)
            pos = end
        else:
            unquoted.append(char)
            pos += 1
    return INPUT_FUNCTION_RE.search("".join(unquoted)) is not None


def returns_result(query):
    words = top_level_words(query)
    first = next(words, None)
    if first == "WITH":
        # `WITH ... SELECT` returns a result, while `WITH ... INSERT` does not.
        return next((word for word in words if word in ("SELECT", "INSERT")), "SELECT") == "SELECT"
    return first in RESULT_KEYWORDS


def format_value(value):
    if value is None:
        return "\\N"
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    return str(value).replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n")


def format_rows(rows):
    return "".join("\t".join(format_value(value) for value in row) + "\n" for row in rows)


class QueryClient:
    """A protocol on which `clickhouse-test` runs the queries one by one."""

    # Whether the settings and the query parameters are passed with the protocol rather than with `SET`.
    native_settings = False
    native_params = False
    # Whether the protocol reports the code of every error, so that it can be checked against the
    # test hint. An error without a code from such a protocol does not satisfy any hint.
    reports_error_codes = True

    def __init__(self, args):
        self.host = args.host
        self.user = args.user
        self.database = args.database
        self.settings = args.settings
        self.params = args.params

    def setup_queries(self):
        return setup_queries(
            [] if self.native_settings else self.settings, [] if self.native_params else self.params
        )

    def connect(self):
        pass

    def execute(self, query):
        """Run the query, return its result as bytes or raise `QueryError`."""
        raise NotImplementedError

    def close(self):
        pass


def setup_queries(settings, params):
    # A changed `compatibility` resets the defaults of the settings that were not changed
    # explicitly, so it goes first, not to undo the others.
    ordered = sorted(settings, key=lambda kv: kv[0] != "compatibility")
    queries = [f"SET {name} = {sql_literal(value)}" for name, value in ordered]
    queries += [f"SET param_{name} = {sql_literal(value)}" for name, value in params]
    return queries


class HTTPClient(QueryClient):
    # The settings and the database of a request override those of the session, so they are set once
    # with `SET` and `USE`, and the `SET` and `USE` of the test persist, as with `clickhouse-client`.
    # The query parameters are passed with every query, as `clickhouse-client` does.
    native_params = True

    def __init__(self, args, secure):
        super().__init__(args)
        self.secure = secure
        self.port = (
            env_port("CLICKHOUSE_PORT_HTTPS", 8443) if secure else env_port("CLICKHOUSE_PORT_HTTP", 8123)
        )
        self.session_id = str(uuid.uuid4())
        self.connection = None

    def setup_queries(self):
        return [f"USE `{self.database}`"] + super().setup_queries()

    def connect(self):
        if self.secure:
            self.connection = http.client.HTTPSConnection(
                self.host, self.port, timeout=SOCKET_TIMEOUT, context=ssl._create_unverified_context()
            )
        else:
            self.connection = http.client.HTTPConnection(self.host, self.port, timeout=SOCKET_TIMEOUT)

    def execute(self, query):
        params = [("session_id", self.session_id)]
        params += [(f"param_{name}", value) for name, value in self.params]
        url = "/?" + urllib.parse.urlencode(params)
        body = query.encode("utf-8", errors="surrogateescape")
        incomplete = False
        try:
            self.connection.request("POST", url, body=body, headers={"X-ClickHouse-User": self.user})
            response = self.connection.getresponse()
            try:
                data = response.read()
            except http.client.IncompleteRead as e:
                # The server may break the response after an exception in the middle of it.
                data = e.partial
                incomplete = True
                self.connection.close()
        except (http.client.HTTPException, OSError):
            # Do not leave a connection broken in the middle of a request for the next query.
            self.connection.close()
            raise
        code = response.getheader("X-ClickHouse-Exception-Code")
        if response.status != 200 or code:
            raise QueryError(data.decode("utf-8", errors="replace"), int(code) if code else None)
        # An exception after the response has started (with `http_wait_end_of_query = 0` or a small
        # `http_response_buffer_size`) is appended to the data as a block tagged with the tag from the
        # headers (`WriteBufferFromHTTPServerResponse::cancelWithException`).
        tag = response.getheader("X-ClickHouse-Exception-Tag")
        if tag and (message := self.tagged_exception(data, tag.encode())) is not None:
            raise QueryError(message)
        if incomplete:
            raise QueryError(f"Incomplete HTTP response without an exception: {data[-1024:]!r}")
        return data

    @staticmethod
    def tagged_exception(data, tag):
        """Return the message of the exception block at the end of the data, if there is one:
        `__exception__\\r\\n<tag>\\r\\n<message>\\n<message size> <tag>\\r\\n__exception__\\r\\n`."""
        header = b"__exception__\r\n" + tag + b"\r\n"
        trailer = b" " + tag + b"\r\n__exception__\r\n"
        if not data.endswith(trailer):
            return None
        start = data.rfind(header)
        if start == -1:
            raise QueryError(f"Malformed exception block in the HTTP response: {data[-1024:]!r}")
        message, _, size = data[start + len(header) : -len(trailer)].rpartition(b"\n")
        message += b"\n"
        if not size.isdigit() or int(size) != len(message):
            raise QueryError(f"Malformed exception block in the HTTP response: {data[-1024:]!r}")
        return message.decode("utf-8", errors="replace")


class MySQLClient(QueryClient):
    """A minimal client of the MySQL text protocol."""

    CLIENT_LONG_PASSWORD = 0x1
    CLIENT_CONNECT_WITH_DB = 0x8
    CLIENT_PROTOCOL_41 = 0x200
    CLIENT_TRANSACTIONS = 0x2000
    CLIENT_SECURE_CONNECTION = 0x8000
    CLIENT_PLUGIN_AUTH = 0x80000
    MAX_PACKET = 0xFFFFFF

    def connect(self):
        self.sock = socket.create_connection(
            (self.host, env_port("CLICKHOUSE_PORT_MYSQL", 9004)), timeout=SOCKET_TIMEOUT
        )
        self.sequence = 0
        handshake = self.read_packet()
        if handshake[0] == 0xFF:
            raise self.error(handshake)

        capabilities = (
            self.CLIENT_LONG_PASSWORD
            | self.CLIENT_CONNECT_WITH_DB
            | self.CLIENT_PROTOCOL_41
            | self.CLIENT_TRANSACTIONS
            | self.CLIENT_SECURE_CONNECTION
            | self.CLIENT_PLUGIN_AUTH
        )
        # The scramble of the handshake is not needed: `mysql_native_password` of the empty password
        # of the test user is empty.
        auth_response = b""
        response = struct.pack("<IIB23x", capabilities, self.MAX_PACKET, 33)
        response += self.user.encode() + b"\0"
        response += bytes([len(auth_response)]) + auth_response
        response += self.database.encode() + b"\0"
        response += b"mysql_native_password\0"
        self.write_packet(response)

        while True:
            packet = self.read_packet()
            if packet[0] == 0x00:
                return
            if packet[0] == 0xFF:
                raise self.error(packet)
            if packet[0] == 0xFE:
                # Auth switch request: the response for the empty password is empty again.
                self.write_packet(auth_response)
                continue
            raise QueryError(f"Unexpected MySQL packet during authentication: {packet[:16]!r}")

    def read_exact(self, size):
        data = bytearray()
        while len(data) < size:
            chunk = self.sock.recv(size - len(data))
            if not chunk:
                raise QueryError("MySQL server closed the connection")
            data += chunk
        return bytes(data)

    def read_packet(self):
        payload = b""
        while True:
            header = self.read_exact(4)
            length = int.from_bytes(header[:3], "little")
            self.sequence = (header[3] + 1) % 256
            payload += self.read_exact(length)
            if length < self.MAX_PACKET:
                return payload

    def write_packet(self, payload):
        while True:
            chunk, payload = payload[: self.MAX_PACKET], payload[self.MAX_PACKET :]
            self.sock.sendall(len(chunk).to_bytes(3, "little") + bytes([self.sequence]) + chunk)
            self.sequence = (self.sequence + 1) % 256
            if len(chunk) < self.MAX_PACKET:
                return

    @staticmethod
    def read_length(packet, pos):
        first = packet[pos]
        if first < 0xFB:
            return first, pos + 1
        if first == 0xFB:
            return None, pos + 1
        size = {0xFC: 2, 0xFD: 3, 0xFE: 8}[first]
        return int.from_bytes(packet[pos + 1 : pos + 1 + size], "little"), pos + 1 + size

    @staticmethod
    def error(packet):
        # ClickHouse sends its own error code as the MySQL error code.
        code = int.from_bytes(packet[1:3], "little")
        message = packet[3:]
        if message[:1] == b"#":
            message = message[6:]
        return QueryError(f"MySQL error {code}: {message.decode('utf-8', errors='replace')}", code)

    @staticmethod
    def is_eof(packet):
        return packet[0] == 0xFE and len(packet) < 9

    def execute(self, query):
        self.sequence = 0
        self.write_packet(b"\x03" + query.encode("utf-8", errors="surrogateescape"))
        packet = self.read_packet()
        if packet[0] == 0x00:
            return b""
        if packet[0] == 0xFF:
            raise self.error(packet)
        column_count, _ = self.read_length(packet, 0)
        for _ in range(column_count):
            self.read_packet()
        if not self.is_eof(packet := self.read_packet()):
            raise QueryError(f"Expected EOF after the MySQL column definitions, got {packet[:16]!r}")
        rows = []
        while True:
            packet = self.read_packet()
            if packet[0] == 0xFF:
                raise self.error(packet)
            if self.is_eof(packet):
                return format_rows(rows).encode()
            row = []
            pos = 0
            while pos < len(packet):
                length, pos = self.read_length(packet, pos)
                if length is None:
                    row.append(None)
                else:
                    row.append(packet[pos : pos + length])
                    pos += length
            rows.append(row)

    def close(self):
        try:
            self.sequence = 0
            self.write_packet(b"\x01")  # COM_QUIT
        except OSError:
            pass
        self.sock.close()


class PostgreSQLClient(QueryClient):
    """A minimal client of the PostgreSQL simple query protocol."""

    # The `ErrorResponse` of a failed query carries a generic SQLSTATE and the text of the exception
    # without its code (`PostgreSQLHandler::sendErrorResponseOrRethrow`), so the codes are compared
    # only for the errors that have the `Code: N.` prefix in their text.
    reports_error_codes = False

    def connect(self):
        self.sock = socket.create_connection(
            (self.host, env_port("CLICKHOUSE_PORT_POSTGRESQL", 9005)), timeout=SOCKET_TIMEOUT
        )
        parameters = b"user\0" + self.user.encode() + b"\0database\0" + self.database.encode() + b"\0\0"
        self.sock.sendall(struct.pack("!II", 8 + len(parameters), 196608) + parameters)
        while True:
            kind, body = self.read_message()
            if kind == b"R":
                code = struct.unpack("!I", body[:4])[0]
                if code == 3:  # Cleartext password; the password of the test user is empty.
                    self.send_message(b"p", b"\0")
                elif code != 0:
                    raise QueryError(f"Unsupported PostgreSQL authentication request {code}")
            elif kind == b"E":
                raise QueryError(self.error_message(body))
            elif kind == b"Z":
                return

    def read_exact(self, size):
        data = bytearray()
        while len(data) < size:
            chunk = self.sock.recv(size - len(data))
            if not chunk:
                raise QueryError("PostgreSQL server closed the connection")
            data += chunk
        return bytes(data)

    def read_message(self):
        header = self.read_exact(5)
        length = struct.unpack("!I", header[1:])[0]
        return header[:1], self.read_exact(length - 4)

    def send_message(self, kind, body):
        self.sock.sendall(kind + struct.pack("!I", len(body) + 4) + body)

    @staticmethod
    def error_message(body):
        fields = {}
        for field in body.split(b"\0"):
            if field:
                fields[field[:1]] = field[1:].decode("utf-8", errors="replace")
        return f"PostgreSQL error {fields.get(b'C', '')}: {fields.get(b'M', '')}"

    def execute(self, query):
        self.send_message(b"Q", query.encode("utf-8", errors="surrogateescape") + b"\0")
        rows = []
        error = None
        while True:
            kind, body = self.read_message()
            if kind == b"D":
                count = struct.unpack("!H", body[:2])[0]
                pos = 2
                row = []
                for _ in range(count):
                    length = struct.unpack("!i", body[pos : pos + 4])[0]
                    pos += 4
                    if length < 0:
                        row.append(None)
                    else:
                        row.append(body[pos : pos + length])
                        pos += length
                rows.append(row)
            elif kind == b"E":
                error = self.error_message(body)
            elif kind == b"Z":
                if error is not None:
                    raise QueryError(error)
                return format_rows(rows).encode()

    def close(self):
        try:
            self.send_message(b"X", b"")
        except OSError:
            pass
        self.sock.close()


class GRPCClient(QueryClient):
    # As with HTTP, the settings and the database of a request override those of the session.

    def setup_queries(self):
        return [f"USE `{self.database}`"] + super().setup_queries()

    def connect(self):
        sys.path.append(
            os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "utils", "grpc-client", "pb2")
        )
        import clickhouse_grpc_pb2  # pylint: disable=import-outside-toplevel
        import clickhouse_grpc_pb2_grpc  # pylint: disable=import-outside-toplevel
        import grpc  # pylint: disable=import-outside-toplevel

        self.pb2 = clickhouse_grpc_pb2
        self.channel = grpc.insecure_channel(
            f"{self.host}:{env_port('CLICKHOUSE_PORT_GRPC', 9100)}",
            options=[("grpc.max_receive_message_length", -1), ("grpc.max_send_message_length", -1)],
        )
        self.stub = clickhouse_grpc_pb2_grpc.ClickHouseStub(self.channel)
        self.session_id = str(uuid.uuid4())

    def execute(self, query):
        result = self.stub.ExecuteQuery(
            self.pb2.QueryInfo(
                query=query,
                session_id=self.session_id,
                user_name=self.user,
                output_format="TabSeparated",
            ),
            timeout=SOCKET_TIMEOUT,
        )
        if result.HasField("exception"):
            raise QueryError(
                f"Code: {result.exception.code}. {result.exception.display_text}", result.exception.code
            )
        return result.output

    def close(self):
        self.channel.close()


class ArrowFlightClient(QueryClient):
    def connect(self):
        import pyarrow  # pylint: disable=import-outside-toplevel
        from pyarrow import flight  # pylint: disable=import-outside-toplevel

        self.pyarrow = pyarrow
        self.flight = flight
        self.client = flight.FlightClient(
            f"grpc://{self.host}:{env_port('CLICKHOUSE_PORT_ARROWFLIGHT', 9090)}"
        )
        token = self.client.authenticate_basic_token(self.user, "")
        self.options = flight.FlightCallOptions(
            headers=[token, (b"x-clickhouse-session-id", str(uuid.uuid4()).encode())],
            timeout=SOCKET_TIMEOUT,
        )
        # The session starts in the default database.
        self.execute(f"USE `{self.database}`")

    def execute(self, query):
        try:
            if returns_result(query):
                table = self.client.do_get(self.flight.Ticket(query.encode()), self.options).read_all()
                columns = [column.to_pylist() for column in table.columns]
                return format_rows(zip(*columns)).encode()
            writer, _ = self.client.do_put(
                self.flight.FlightDescriptor.for_command(query), self.pyarrow.schema([]), self.options
            )
            writer.close()
            return b""
        except self.pyarrow.ArrowException as e:
            raise QueryError(str(e)) from e

    def close(self):
        self.client.close()


def is_binary(data):
    """Whether the test contains bytes that only `BINARY_PROTOCOLS` can carry."""
    if b"\0" in data:
        return True
    try:
        data.decode("utf-8")
    except UnicodeDecodeError:
        return True
    return False


# A value of the `dialect` setting, e.g. in `SET dialect = 'kusto'`.
DIALECT_RE = re.compile(r"""\bdialect\s*=\s*['"`]?(\w+)""", re.IGNORECASE)


def uses_other_dialect(text):
    """Whether the test switches to a dialect other than `clickhouse`. `split_queries` knows only the
    statement boundaries of SQL: e.g. a `let` binding of KQL belongs to the following tabular expression,
    so splitting `let F = (a:long) { a }; print F(5);` at the semicolon breaks the test."""
    return any(match.group(1).lower() != "clickhouse" for match in DIALECT_RE.finditer(text))


def supports(protocol, data):
    """Whether the test with the contents `data` can run over the protocol without changing its queries.
    `clickhouse-test` chooses the protocol of a test among the ones that support it."""
    if is_binary(data) and protocol not in BINARY_PROTOCOLS:
        return False
    # The same byte-preserving decoding as in `main`: `BINARY_PROTOCOLS` carry invalid UTF-8 as is.
    text = data.decode("utf-8", errors="surrogateescape")
    # Only `ssh` passes the whole test to `ClientBase`, which splits it according to the dialect.
    if protocol != "ssh" and uses_other_dialect(text):
        return False
    if protocol == "ssh":
        return len(data) <= SSH_MAX_TEST_SIZE
    if protocol == "webterminal":
        # The line editor removes the whitespace at the end of the pasted text, which can be a field
        # separator or a part of a value at the end of the inline data of `INSERT ... FORMAT`.
        return all(query == query.rstrip() for query, _ in split_queries(text))
    return True


def prepend_setup(text, setup):
    return "".join(query + ";\n" for query in setup) + text


def run_ssh(args, text):
    """Pass the whole test to the embedded client as the command of an SSH channel, like
    `ssh user@host "SELECT 1"`. The embedded client splits it into queries and applies the hints."""
    text = prepend_setup(text, [f"USE `{args.database}`"] + setup_queries(args.settings, args.params))
    if len(text.encode("utf-8", errors="surrogateescape")) > SSH_MAX_QUERY_SIZE:
        raise QueryError(f"The test is larger than {SSH_MAX_QUERY_SIZE} bytes, it cannot be run over SSH")
    # OpenSSH refuses a private key that is readable by others, and git checks it out as 644.
    key_dir = tempfile.mkdtemp(prefix="clickhouse-test-ssh-")
    try:
        key = os.path.join(key_dir, "key")
        shutil.copyfile(args.ssh_key, key)
        os.chmod(key, 0o600)
        command = [
            "ssh",
            "-o", "StrictHostKeyChecking=no",
            "-o", "UserKnownHostsFile=/dev/null",
            "-o", "BatchMode=yes",
            "-o", "LogLevel=ERROR",
            "-o", "ConnectTimeout=60",
            "-o", "ServerAliveInterval=30",
            "-o", "ServerAliveCountMax=20",
            "-i", key,
            "-p", str(env_port("CLICKHOUSE_PORT_SSH", 9022)),
            f"{args.ssh_user}@{args.host}",
            text,
        ]
        # The random disruptor of the Stress test terminates this process to disconnect the client in
        # the middle of a query. The connection belongs to the `ssh` child, so terminate it as well.
        previous_handler = signal.signal(signal.SIGTERM, lambda signum, frame: sys.exit(128 + signum))
        try:
            process = subprocess.Popen(command, stdin=subprocess.DEVNULL)
            try:
                return process.wait()
            finally:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
        finally:
            signal.signal(signal.SIGTERM, previous_handler)
    finally:
        shutil.rmtree(key_dir)


class WebTerminal:
    """Pastes the queries into the interactive embedded client behind `/webterminal` one by one,
    and waits for the prompt after every query, like a person at the browser would."""

    PROMPT = b":) "
    # Replxx enables the bracketed paste mode each time it starts reading a new line, right before it
    # prints the prompt. It distinguishes the prompt from `:) ` in the echoed queries and their results.
    PROMPT_START = b"\x1b[?2004h"
    # A bracketed paste keeps the text as is, line breaks included, and the line break after it
    # submits the whole text. The client splits it into queries and applies the test hints itself.
    PASTE_START = b"\x1b[200~"
    PASTE_END = b"\x1b[201~"
    # What the interactive client prints, instead of stopping, on an error that no hint expects
    # (`ClientEmbedded::processError`) and on an error or its absence that does not match the hint
    # (`ClientBase::executeMultiQuery`).
    ERROR_RE = re.compile(
        rb"Received exception\r?\n|(Expected |The query succeeded but )[^\r\n]*\berrors?\b[^\r\n]*\(query: "
    )

    def __init__(self, args):
        self.host = args.host
        self.port = env_port("CLICKHOUSE_PORT_HTTP", 8123)
        self.user = args.user

    def connect(self):
        self.sock = socket.create_connection((self.host, self.port), timeout=SOCKET_TIMEOUT)
        key = base64.b64encode(os.urandom(16)).decode()
        host = f"{self.host}:{self.port}"
        request = (
            f"GET /webterminal HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\nOrigin: http://{host}\r\n\r\n"
        )
        self.sock.sendall(request.encode())
        response = b""
        while b"\r\n\r\n" not in response:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise QueryError("The server closed the connection during the WebSocket handshake")
            response += chunk
        head, self.pending = response.split(b"\r\n\r\n", 1)
        if not head.startswith(b"HTTP/1.1 101"):
            raise QueryError(f"WebSocket handshake failed: {head.decode(errors='replace')}")
        expected = base64.b64encode(
            hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()
        )
        if b"Sec-WebSocket-Accept: " + expected not in head:
            raise QueryError("WebSocket handshake returned a wrong Sec-WebSocket-Accept")
        self.send_frame(0x1, json.dumps({"type": "auth", "user": self.user, "password": ""}).encode())
        # A wide terminal, so that the client does not wrap and redraw long queries.
        self.send_frame(0x1, json.dumps({"type": "resize", "cols": 500, "rows": 50}).encode())
        self.wait_for_prompt()

    def send_frame(self, opcode, payload):
        header = bytearray([0x80 | opcode])
        if len(payload) < 126:
            header.append(0x80 | len(payload))
        elif len(payload) < 65536:
            header.append(0x80 | 126)
            header += struct.pack("!H", len(payload))
        else:
            header.append(0x80 | 127)
            header += struct.pack("!Q", len(payload))
        mask = os.urandom(4)
        header += mask
        self.sock.sendall(bytes(header) + bytes(b ^ mask[i % 4] for i, b in enumerate(payload)))

    def recv_exact(self, size):
        while len(self.pending) < size:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise EOFError
            self.pending += chunk
        data, self.pending = self.pending[:size], self.pending[size:]
        return data

    def read_frame(self):
        first, second = self.recv_exact(2)
        length = second & 0x7F
        if length == 126:
            length = struct.unpack("!H", self.recv_exact(2))[0]
        elif length == 127:
            length = struct.unpack("!Q", self.recv_exact(8))[0]
        return first & 0x0F, self.recv_exact(length)

    def read_output(self):
        """Read the next chunk of the terminal output, return `None` when the session is over."""
        try:
            opcode, payload = self.read_frame()
        except EOFError:
            return None
        if opcode == 0x8:
            if len(payload) >= 2 and struct.unpack("!H", payload[:2])[0] != 1000:
                sys.stderr.write(f"Web terminal closed: {payload[2:].decode(errors='replace')}\n")
            return None
        if opcode == 0x9:
            self.send_frame(0xA, payload)
            return b""
        sys.stdout.buffer.write(payload)
        return payload

    def wait_for_prompt(self):
        """Read the output until the next prompt, return the output before the prompt."""
        output = bytearray()
        while True:
            start = output.rfind(self.PROMPT_START)
            # Nothing follows the prompt until the next input.
            if start != -1 and output.endswith(self.PROMPT) and b"\n" not in output[start:]:
                return output[:start]
            data = self.read_output()
            if data is None:
                raise QueryError("The web terminal session ended before the prompt")
            output += data

    def run(self, queries):
        """Run the queries, return whether all of them succeeded or failed as their hints expect."""
        for query in queries:
            text = query.encode("utf-8", errors="surrogateescape")
            self.send_frame(0x2, self.PASTE_START + text + self.PASTE_END + b"\r")
            if self.ERROR_RE.search(self.wait_for_prompt()):
                self.sock.close()
                return False
        self.send_frame(0x2, b"exit\r")
        output = bytearray()
        while (data := self.read_output()) is not None:
            output += data
        self.sock.close()
        return not self.ERROR_RE.search(output)


def run_webterminal(args, text):
    terminal = WebTerminal(args)
    terminal.connect()
    setup = [f"USE `{args.database}`"] + setup_queries(args.settings, args.params)
    # A query goes with its hint after the semicolon. A query without a hint goes without the
    # semicolon, because the data of `INSERT ... FORMAT` would take it as a part of the data.
    queries = [f"{query}; {hint}" if hint else query for query, hint in split_queries(text)]
    return 0 if terminal.run(setup + queries) else 1


def run_queries(client, text, error_codes):
    client.connect()
    try:
        for query in client.setup_queries():
            client.execute(query)
        for query, hint in split_queries(text):
            expected = expected_errors(query, hint)
            try:
                output = client.execute(query)
            except QueryError as e:
                if expected is None:
                    sys.stderr.write(f"Error on query {query!r}:\n{e}\n")
                    return 1
                if mismatch := check_error(client, e, expected, error_codes):
                    sys.stderr.write(f"{mismatch}\nQuery: {query!r}\n")
                    return 1
                continue
            sys.stdout.buffer.write(output)
            if expected is not None:
                sys.stderr.write(f"Expected an error, but the query succeeded: {query!r}\n")
                return 1
        return 0
    finally:
        client.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--protocol", required=True, choices=PROTOCOLS)
    parser.add_argument("--host", default=os.environ.get("CLICKHOUSE_HOST", "localhost"))
    parser.add_argument("--user", default="default")
    parser.add_argument("--database", default="default")
    parser.add_argument("--setting", dest="settings", action="append", default=[], help="name=value")
    parser.add_argument("--param", dest="params", action="append", default=[], help="name=value")
    parser.add_argument("--ssh-user", help="The user with the key authentication for the ssh protocol")
    parser.add_argument(
        "--ssh-key",
        default=os.path.join(os.path.dirname(os.path.realpath(__file__)), "config", "ssh_user_ed25519_key"),
    )
    args = parser.parse_args()
    args.settings = parse_key_values(args.settings)
    args.params = parse_key_values(args.params)

    text = sys.stdin.buffer.read().decode("utf-8", errors="surrogateescape")
    try:
        if args.protocol == "ssh":
            if not args.ssh_user:
                parser.error("--ssh-user is required for the ssh protocol")
            return run_ssh(args, text)
        if args.protocol == "webterminal":
            return run_webterminal(args, text)
        client = {
            "http": lambda: HTTPClient(args, secure=False),
            "https": lambda: HTTPClient(args, secure=True),
            "mysql": lambda: MySQLClient(args),
            "postgresql": lambda: PostgreSQLClient(args),
            "grpc": lambda: GRPCClient(args),
            "arrowflight": lambda: ArrowFlightClient(args),
        }[args.protocol]()
        return run_queries(client, text, ErrorCodes(args.host))
    except QueryError as e:
        sys.stderr.write(f"{e}\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
