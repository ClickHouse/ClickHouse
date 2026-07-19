import logging
import re
import socket
import struct
import time

import pymysql
import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node_ddl = cluster.add_instance(
    "node_audit_ddl",
    main_configs=["configs/logger_audit_ddl.xml"],
    stay_alive=True,
)
node_dml_misc = cluster.add_instance(
    "node_audit_dml_misc",
    main_configs=["configs/logger_audit_dml_misc.xml"],
    stay_alive=True,
)
node_user_dcl = cluster.add_instance(
    "node_audit_user_dcl",
    main_configs=["configs/logger_audit_user_dcl.xml"],
    stay_alive=True,
)
node_mysql_ssl_required = cluster.add_instance(
    "node_audit_mysql_ssl_required",
    main_configs=["configs/logger_audit_mysql_ssl_required.xml"],
    stay_alive=True,
)
node_whitespace_types = cluster.add_instance(
    "node_audit_whitespace_types",
    main_configs=["configs/logger_audit_whitespace_types.xml"],
    stay_alive=True,
)
node_reload = cluster.add_instance(
    "node_audit_reload",
    main_configs=["configs/logger_audit_reload.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def assert_audit_log_contain_with_retry(instance, substring, retry_count=20, sleep_time=0.5):
    for i in range(retry_count):
        try:
            if instance.contains_in_log(substring, from_host=True, filename="clickhouse-server.audit.log"):
                break
            time.sleep(sleep_time)
        except Exception as ex:
            logging.exception(f"assert_audit_log_contain_with_retry retry {i+1} exception {ex}")
            time.sleep(sleep_time)
    else:
        raise AssertionError("'{}' not found in audit logs".format(substring))


def assert_audit_log_count_with_retry(instance, expected, retry_count=20, sleep_time=0.5):
    last_count = None
    for i in range(retry_count):
        try:
            last_count = instance.count_log_lines("/var/log/clickhouse-server/clickhouse-server.audit.log")
            if last_count == expected:
                break
            time.sleep(sleep_time)
        except Exception as ex:
            logging.exception(f"assert_audit_log_count_with_retry retry {i+1} exception {ex}")
            time.sleep(sleep_time)
    else:
        raise AssertionError(
            "expected {} audit log lines, got {}".format(expected, last_count)
        )


def test_audit_log_filter(start_cluster):
    node_ddl.query("DROP TABLE IF EXISTS test_audit_log")
    node_ddl.query("CREATE TABLE test_audit_log(a int) ENGINE=Memory")
    node_ddl.query("USE default")
    node_ddl.query("INSERT INTO test_audit_log VALUES (0),(1),(2)")
    node_ddl.query("SELECT a FROM test_audit_log")

    # Audit DDL queries
    assert_audit_log_contain_with_retry(node_ddl, "DDL")

    # Audit only DDL queries
    assert_audit_log_count_with_retry(node_ddl, 2)

    node_dml_misc.query("DROP TABLE IF EXISTS test_audit_log")
    node_dml_misc.query("CREATE TABLE test_audit_log(a int) ENGINE=Memory")
    node_dml_misc.query("USE default")
    node_dml_misc.query("INSERT INTO test_audit_log VALUES (0),(1),(2)")
    node_dml_misc.query("SELECT a FROM test_audit_log")

    # Audit DML and MISC queries
    assert_audit_log_contain_with_retry(node_dml_misc, "DML")
    assert_audit_log_contain_with_retry(node_dml_misc, "MISC")

    # Filter DDL queries
    assert not node_dml_misc.contains_in_log("DDL", from_host=True, filename="clickhouse-server.audit.log")

    # Audit USER login/logout
    node_user_dcl.query("SELECT 1")
    assert_audit_log_contain_with_retry(node_user_dcl, "LoginSuccess")
    assert_audit_log_contain_with_retry(node_user_dcl, "Logout")

    node_user_dcl.query("CREATE USER user1")
    node_user_dcl.query("GRANT SELECT ON t_not_exists TO user1")
    node_user_dcl.query("REVOKE ALL ON t_not_exists FROM user1")

    assert_audit_log_contain_with_retry(node_user_dcl, "DCL")
    assert_audit_log_contain_with_retry(node_user_dcl, "GRANT")

def test_audit_log_object_names(start_cluster):
    node_dml_misc.query("CREATE DATABASE IF NOT EXISTS test")
    node_dml_misc.query("DROP TABLE IF EXISTS test.t_audit")
    node_dml_misc.query("CREATE TABLE test.t_audit(a int) ENGINE=Memory")
    node_dml_misc.query("INSERT INTO test.t_audit VALUES (0),(1),(2)")

    assert_audit_log_contain_with_retry(node_dml_misc, "test.t_audit")
    log_content = node_dml_misc.grep_in_log("test.t_audit", from_host=True, filename="clickhouse-server.audit.log")
    assert "DML" in log_content


def test_audit_log_newline_escaping(start_cluster):
    """Queries with embedded newlines must appear as a single audit log line.
    Verify that line-comment content and a forged AUDIT:-looking suffix
    cannot escape into a separate physical line."""
    node_dml_misc.query("SELECT 1\n-- injected\\nAUDIT: DDL, Drop, 0, hacker, 1.2.3.4, db.t, DROP TABLE t\nFORMAT Null")

    assert_audit_log_contain_with_retry(node_dml_misc, "SELECT 1")
    log_content = node_dml_misc.grep_in_log("SELECT 1", from_host=True, filename="clickhouse-server.audit.log")
    matching_lines = [line for line in log_content.strip().split("\n") if "SELECT 1" in line]
    assert len(matching_lines) == 1, f"Expected exactly one audit line with SELECT 1, got {len(matching_lines)}"
    audit_line = matching_lines[0]
    assert "injected" in audit_line, "Multi-line query must be collapsed into a single log line"
    assert "FORMAT Null" in audit_line, "Content after the comment must remain on the same physical line"

    # Ensure the forged AUDIT: prefix did not start a separate line.
    # When escaping works, "hacker" only appears on the same line as "SELECT 1"
    # (as part of the comment text). A separate line with "hacker" but without
    # "SELECT 1" would mean a newline escaped and created a forged record.
    all_lines = node_dml_misc.grep_in_log("AUDIT", from_host=True, filename="clickhouse-server.audit.log")
    for line in all_lines.strip().split("\n"):
        if "hacker" in line and "Drop" in line and "SELECT 1" not in line:
            raise AssertionError("Forged AUDIT record appeared as a separate log line — newline escaping is broken")


def test_audit_log_login_failure_has_client_ip(start_cluster):
    """Failed login attempts (unknown user) must record the real client IP."""
    try:
        node_user_dcl.query("SELECT 1", user="nonexistent_user_12345", password="wrong")
    except Exception:
        pass

    assert_audit_log_contain_with_retry(node_user_dcl, "LoginFailure")
    log_content = node_user_dcl.grep_in_log("LoginFailure", from_host=True, filename="clickhouse-server.audit.log")
    assert "nonexistent_user_12345" in log_content
    assert "Unknown Host" not in log_content, "Pre-auth failures must include the real client IP, not 'Unknown Host'"


def pg_startup_message(user, database=""):
    """Build a PostgreSQL v3 StartupMessage for the given user."""
    # Protocol version 3.0
    proto = struct.pack("!HH", 3, 0)
    payload = proto
    payload += b"user\x00" + user.encode() + b"\x00"
    if database:
        payload += b"database\x00" + database.encode() + b"\x00"
    payload += b"\x00"  # terminator
    # Length includes itself (4 bytes) but not the initial type byte (StartupMessage has none)
    length = struct.pack("!I", len(payload) + 4)
    return length + payload


def mysql_handshake_response(username, auth_response=b"", database="", auth_plugin_name="mysql_native_password", capability_flags=None):
    """Build a MySQL HandshakeResponse41 packet with the given username.

    This constructs a valid protocol-level response that the server can parse
    to extract the username, which is needed for proper audit logging.
    """
    CLIENT_PROTOCOL_41 = 0x00000200
    CLIENT_SECURE_CONNECTION = 0x00008000
    CLIENT_PLUGIN_AUTH = 0x00080000
    CLIENT_CONNECT_WITH_DB = 0x00000008

    if capability_flags is None:
        capability_flags = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH
        if database:
            capability_flags |= CLIENT_CONNECT_WITH_DB

    max_packet_size = 16777216
    character_set = 33  # utf8_general_ci

    payload = struct.pack("<I", capability_flags)
    payload += struct.pack("<I", max_packet_size)
    payload += struct.pack("<B", character_set)
    payload += b"\x00" * 23  # reserved
    payload += username.encode() + b"\x00"
    # auth_response with length prefix (CLIENT_SECURE_CONNECTION)
    payload += struct.pack("<B", len(auth_response)) + auth_response
    if database and (capability_flags & CLIENT_CONNECT_WITH_DB):
        payload += database.encode() + b"\x00"
    if capability_flags & CLIENT_PLUGIN_AUTH:
        payload += auth_plugin_name.encode() + b"\x00"

    # MySQL packet header: 3-byte length (LE) + 1-byte sequence id
    header = struct.pack("<I", len(payload))[:3] + struct.pack("<B", 1)
    return header + payload


def test_audit_log_mysql_wrong_password(start_cluster):
    """When MySQL authentication fails with wrong password, must emit LoginFailure with client IP."""
    unique_user = "mysql_wrong_pass_user"

    # Create a user with double_sha1_password authentication (supported by MySQL protocol)
    node_user_dcl.query(
        f"CREATE USER IF NOT EXISTS {unique_user} IDENTIFIED WITH double_sha1_password BY 'correct_password'"
    )

    # Wait for user to be created
    time.sleep(1)

    try:
        # Connect via pymysql to the MySQL wire port with wrong password
        pymysql.connections.Connection(
            host=node_user_dcl.ip_address,
            user=unique_user,
            password="wrongpassword",
            database="default",
            port=9004,
        )
    except Exception:
        pass  # Expected to fail
    finally:
        # Clean up
        node_user_dcl.query(f"DROP USER IF EXISTS {unique_user}")

    # Verify LoginFailure was logged with the username and client IP
    assert_audit_log_contain_with_retry(node_user_dcl, unique_user)
    log_content = node_user_dcl.grep_in_log(unique_user, from_host=True, filename="clickhouse-server.audit.log")
    assert "LoginFailure" in log_content, "LoginFailure must be emitted for failed MySQL authentication"
    assert "Unknown Host" not in log_content, "Client IP must be recorded for auth failure, not 'Unknown Host'"


def test_audit_log_tcp_preauth_failure(start_cluster):
    """TCP pre-authentication failures (wrong password) must record the real client IP.

    This test covers the authentication failure path for the native TCP protocol,
    including scenarios that could occur before full authentication completes."""
    # Create a user with a specific password
    node_user_dcl.query(
        "CREATE USER IF NOT EXISTS tcp_test_user IDENTIFIED BY 'correct_password'"
    )

    # Wait for user to be created
    time.sleep(1)

    try:
        # Try to connect with wrong password to trigger pre-auth failure
        node_user_dcl.exec_in_container(
            [
                "bash",
                "-c",
                "clickhouse client --host 127.0.0.1 --port 9000 "
                "--user tcp_test_user --password wrongpassword --query 'SELECT 1' 2>&1 || true"
            ],
            privileged=True
        )
    except Exception:
        pass  # Expected to fail
    finally:
        # Clean up
        node_user_dcl.query("DROP USER IF EXISTS tcp_test_user")

    # Verify LoginFailure was logged with the username and client IP
    assert_audit_log_contain_with_retry(node_user_dcl, "tcp_test_user")
    log_content = node_user_dcl.grep_in_log("tcp_test_user", from_host=True, filename="clickhouse-server.audit.log")
    assert "LoginFailure" in log_content, "LoginFailure must be emitted for failed authentication"
    assert "Unknown Host" not in log_content, "Client IP must be recorded for auth failure, not 'Unknown Host'"


def test_audit_log_whitespace_only_types_defaults_to_ddl(start_cluster):
    """Whitespace-only or separator-only auditlog_types must default to DDL,
    not silently disable all audit categories."""
    node_whitespace_types.query("DROP TABLE IF EXISTS test_whitespace_audit")
    node_whitespace_types.query("CREATE TABLE test_whitespace_audit(a int) ENGINE=Memory")

    assert_audit_log_contain_with_retry(node_whitespace_types, "DDL")
    assert_audit_log_count_with_retry(node_whitespace_types, 2)


def test_audit_log_truncate_classified_as_dml(start_cluster):
    """TRUNCATE TABLE is a data-modifying operation and must be classified as DML,
    not DDL (even though it uses ASTDropQuery with QueryKind::Drop internally)."""
    node_dml_misc.query("DROP TABLE IF EXISTS test_truncate_audit")
    node_dml_misc.query("CREATE TABLE test_truncate_audit(a int) ENGINE=Memory")
    node_dml_misc.query("INSERT INTO test_truncate_audit VALUES (1),(2),(3)")
    node_dml_misc.query("TRUNCATE TABLE test_truncate_audit")

    assert_audit_log_contain_with_retry(node_dml_misc, "TRUNCATE TABLE test_truncate_audit")
    log_content = node_dml_misc.grep_in_log("TRUNCATE TABLE test_truncate_audit", from_host=True, filename="clickhouse-server.audit.log")
    assert "DML" in log_content, "TRUNCATE must be classified as DML, not DDL"
    assert "DDL" not in log_content, "TRUNCATE must not appear as DDL"


def test_audit_log_ddl_object_names_without_log_queries(start_cluster):
    """DDL audit records must include OBJECT_NAMES even when log_queries=0.
    The object-name extraction must not depend on the system.query_log write path."""
    node_ddl.query("""SET log_queries=0;
                    DROP TABLE IF EXISTS test_no_log_queries_audit;
                    CREATE TABLE test_no_log_queries_audit(a int) ENGINE=Memory """)

    assert_audit_log_contain_with_retry(node_ddl, "test_no_log_queries_audit")
    log_content = node_ddl.grep_in_log("test_no_log_queries_audit", from_host=True, filename="clickhouse-server.audit.log")
    assert "DDL" in log_content, "DDL audit record must be present"
    lines = [line for line in log_content.strip().split("\n") if "CREATE TABLE" in line and "test_no_log_queries_audit" in line]
    assert len(lines) >= 1, "CREATE TABLE audit record must mention the table name in OBJECT_NAMES"


def test_audit_log_check_grant_classified_as_dcl(start_cluster):
    """CHECK GRANT is an access-control statement and must be classified as DCL,
    not DML (even though ASTCheckGrantQuery returns QueryKind::Check)."""
    node_user_dcl.query("CREATE USER IF NOT EXISTS check_grant_test_user")
    node_user_dcl.query("GRANT SELECT ON default.* TO check_grant_test_user")

    # CHECK GRANT should appear as DCL in the audit log (node_user_dcl has DCL enabled)
    node_user_dcl.query("CHECK GRANT SELECT ON default.*", user="check_grant_test_user")

    assert_audit_log_contain_with_retry(node_user_dcl, "CHECK GRANT")
    log_content = node_user_dcl.grep_in_log("CHECK GRANT", from_host=True, filename="clickhouse-server.audit.log")
    assert "DCL" in log_content, "CHECK GRANT must be classified as DCL"
    assert "DML" not in log_content, "CHECK GRANT must not appear as DML"

    node_user_dcl.query("DROP USER IF EXISTS check_grant_test_user")


def test_audit_log_database_object_names(start_cluster):
    """Database-level DDL (CREATE/DROP DATABASE) must record the database name in OBJECT_NAMES.
    These statements populate only query_databases (no table/view names), so the affected object
    name must be taken from there as well."""
    node_ddl.query("DROP DATABASE IF EXISTS audit_db_objnames")
    node_ddl.query("CREATE DATABASE audit_db_objnames")
    node_ddl.query("DROP DATABASE audit_db_objnames")

    assert_audit_log_contain_with_retry(node_ddl, "audit_db_objnames")
    log_content = node_ddl.grep_in_log("audit_db_objnames", from_host=True, filename="clickhouse-server.audit.log")
    lines = [line for line in log_content.strip().split("\n") if "audit_db_objnames" in line]

    create_lines = [line for line in lines if "Create" in line]
    drop_lines = [line for line in lines if "Drop" in line]
    assert len(create_lines) >= 1, "CREATE DATABASE must produce a DDL audit record"
    assert len(drop_lines) >= 1, "DROP DATABASE must produce a DDL audit record"

    # The database name must appear in OBJECT_NAMES *in addition to* the query text, so it occurs
    # at least twice on the line. Before the fix OBJECT_NAMES was empty (database name only in the
    # query text, i.e. exactly once).
    assert create_lines[0].count("audit_db_objnames") >= 2, \
        f"CREATE DATABASE must record the database in OBJECT_NAMES: {create_lines[0]}"
    assert drop_lines[0].count("audit_db_objnames") >= 2, \
        f"DROP DATABASE must record the database in OBJECT_NAMES: {drop_lines[0]}"
    for line in create_lines + drop_lines:
        assert "DDL" in line, "CREATE/DROP DATABASE must be classified as DDL"


def test_audit_log_failed_query_before_start(start_cluster):
    """Queries that fail before execution starts (e.g. a parse error) must still be audited.
    Such queries never reach logQueryFinish/logQueryException, so the audit record has to be
    emitted from logExceptionBeforeStart. A parse error has no AST and is classified as MISC.

    The query is sent over HTTP on purpose: clickhouse-client parses the statement locally, so
    a syntax error would be rejected on the client and never reach the server, producing no
    server-side audit record. The HTTP interface parses server-side, exercising the audit path."""
    error = node_dml_misc.http_query_and_get_error("SELECT 'audit_before_start_marker' FROM")
    assert error, "The malformed query must be rejected by the server"

    assert_audit_log_contain_with_retry(node_dml_misc, "audit_before_start_marker")
    log_content = node_dml_misc.grep_in_log("audit_before_start_marker", from_host=True, filename="clickhouse-server.audit.log")
    lines = [line for line in log_content.strip().split("\n") if "audit_before_start_marker" in line and "AUDIT:" in line]
    assert len(lines) >= 1, "A query that fails before start must produce an audit record"

    # Audit message format: "TYPE, COMMAND, EXCEPTION_CODE, USER, IP, OBJECT_NAMES, QUERY".
    audit_part = lines[0].split("AUDIT: ", 1)[1]
    fields = audit_part.split(", ")
    assert fields[0] == "MISC", f"A parse error (no AST) must be classified as MISC: {lines[0]}"
    assert fields[2] != "0", f"A failed query must record a non-zero exception code: {lines[0]}"


def test_audit_log_failed_ddl_object_names(start_cluster):
    """A DDL statement that fails before execution starts (e.g. RENAME of a missing table) is
    audited from logExceptionBeforeStart, where query_tables is not populated. The affected
    object name must still be recorded in OBJECT_NAMES, extracted from the AST, so a failed
    sensitive operation is not audited with an empty OBJECT_NAMES field."""
    missing = "audit_missing_rename_src"
    dest = "audit_missing_rename_dst"
    node_ddl.query(f"DROP TABLE IF EXISTS {missing}")
    node_ddl.query(f"DROP TABLE IF EXISTS {dest}")

    error = node_ddl.query_and_get_error(f"RENAME TABLE {missing} TO {dest}")
    assert error, "RENAME of a missing table must fail"

    assert_audit_log_contain_with_retry(node_ddl, missing)
    log_content = node_ddl.grep_in_log(missing, from_host=True, filename="clickhouse-server.audit.log")
    lines = [line for line in log_content.strip().split("\n")
             if "AUDIT:" in line and "Rename" in line and missing in line]
    assert len(lines) >= 1, "A failed RENAME must produce a DDL audit record"

    # Audit message format: "TYPE, COMMAND, EXCEPTION_CODE, USER, IP, OBJECT_NAMES, QUERY".
    audit_part = lines[0].split("AUDIT: ", 1)[1]
    fields = audit_part.split(", ")
    assert fields[0] == "DDL", f"RENAME must be classified as DDL: {lines[0]}"
    assert fields[2] != "0", f"A failed RENAME must record a non-zero exception code: {lines[0]}"
    # OBJECT_NAMES is field index 5; it must carry the affected table, not only the query text.
    assert missing in fields[5], f"OBJECT_NAMES must record the RENAME source table: {lines[0]}"


def test_audit_log_failed_select_object_names(start_cluster):
    """A select-like DML statement that fails before execution starts (e.g. a SELECT from a missing
    table) is audited from logExceptionBeforeStart, where query_tables is not populated. The
    referenced table must still be recorded in OBJECT_NAMES, extracted from the AST, so a failed
    read is not audited with an empty OBJECT_NAMES field."""
    missing = "audit_missing_select_table"
    node_dml_misc.query(f"DROP TABLE IF EXISTS {missing}")

    error = node_dml_misc.query_and_get_error(f"SELECT * FROM {missing}")
    assert error, "SELECT from a missing table must fail"

    assert_audit_log_contain_with_retry(node_dml_misc, missing)
    log_content = node_dml_misc.grep_in_log(missing, from_host=True, filename="clickhouse-server.audit.log")
    lines = [line for line in log_content.strip().split("\n")
             if "AUDIT:" in line and "Select" in line and missing in line]
    assert len(lines) >= 1, "A failed SELECT must produce a DML audit record"

    # Audit message format: "TYPE, COMMAND, EXCEPTION_CODE, USER, IP, OBJECT_NAMES, QUERY".
    audit_part = lines[0].split("AUDIT: ", 1)[1]
    fields = audit_part.split(", ")
    assert fields[0] == "DML", f"SELECT must be classified as DML: {lines[0]}"
    assert fields[2] != "0", f"A failed SELECT must record a non-zero exception code: {lines[0]}"
    # OBJECT_NAMES is field index 5; it must carry the referenced table, not only the query text.
    assert missing in fields[5], f"OBJECT_NAMES must record the SELECT source table: {lines[0]}"


def test_audit_log_comma_in_query_is_parseable(start_cluster):
    """An audit record is a list of comma-separated fields, so a comma inside a free-form field
    (the query text, an object name, or a username) must be escaped — otherwise the record cannot
    be split back into its fields. Verify that a query containing a comma stays a single,
    machine-parseable record whose comma is escaped as '\\,'."""
    marker = "audit_comma_marker"
    node_dml_misc.query(f"SELECT 1 AS {marker}, 2")

    assert_audit_log_contain_with_retry(node_dml_misc, marker)
    log_content = node_dml_misc.grep_in_log(marker, from_host=True, filename="clickhouse-server.audit.log")
    lines = [line for line in log_content.strip().split("\n") if "AUDIT:" in line and marker in line]
    assert len(lines) >= 1, "The query with a comma must produce an audit record"

    audit_part = lines[0].split("AUDIT: ", 1)[1]
    # Split on unescaped separators only: a literal comma inside a field is written as '\,'.
    fields = re.split(r"(?<!\\), ", audit_part)
    assert len(fields) == 7, f"An escaped record must split into exactly 7 fields: {fields}"
    assert fields[0] == "DML", f"SELECT must be classified as DML: {lines[0]}"
    # The literal comma in the query text must survive as an escaped comma in the QUERY field, so
    # it does not masquerade as a field separator.
    assert marker in fields[6], f"QUERY field must contain the query text: {lines[0]}"
    assert "\\," in fields[6], f"A comma in the query text must be escaped in the audit record: {lines[0]}"


def test_audit_log_pg_unsupported_auth_method(start_cluster):
    """A user whose only authentication method cannot be used by the PostgreSQL wire protocol
    (for example sha256_password) must still produce a LoginFailure audit record when a
    PostgreSQL client attempts to log in. This exercises the unsupported-method negotiation path
    that throws before Session::authenticate is reached."""
    unique_user = "pg_unsupported_method_user"
    node_user_dcl.query(f"DROP USER IF EXISTS {unique_user}")
    node_user_dcl.query(f"CREATE USER {unique_user} IDENTIFIED WITH sha256_password BY 'secret'")
    time.sleep(1)

    try:
        s = socket.create_connection((node_user_dcl.ip_address, 9005), timeout=10)
        try:
            s.sendall(pg_startup_message(unique_user))
            s.settimeout(5)
            s.recv(4096)  # read the server's "authentication method is not supported" error
        finally:
            s.close()
    except Exception:
        pass  # The connection is expected to be rejected
    finally:
        node_user_dcl.query(f"DROP USER IF EXISTS {unique_user}")

    assert_audit_log_contain_with_retry(node_user_dcl, unique_user)
    log_content = node_user_dcl.grep_in_log(unique_user, from_host=True, filename="clickhouse-server.audit.log")
    assert "LoginFailure" in log_content, "LoginFailure must be emitted for a PostgreSQL unsupported-method auth failure"
    assert "Unknown Host" not in log_content, "Client IP must be recorded for the auth failure, not 'Unknown Host'"


AUDIT_LOG_CONFIG_PATH = "/etc/clickhouse-server/config.d/logger_audit_reload.xml"
AUDIT_LOG_FILE = "/var/log/clickhouse-server/clickhouse-server.audit.log"


def test_audit_log_reload_enable(start_cluster):
    """Starting with allow_experimental_audit_log=false, a config reload to true
    must start emitting audit records without a server restart."""

    # The node starts with allow_experimental_audit_log=false.
    # Run a DDL — no audit record should be written.
    node_reload.query("DROP TABLE IF EXISTS test_reload_before")
    node_reload.query("CREATE TABLE test_reload_before(a int) ENGINE=Memory")
    time.sleep(1)

    audit_exists = node_reload.exec_in_container(
        ["bash", "-c", f"test -f {AUDIT_LOG_FILE} && wc -l < {AUDIT_LOG_FILE} || echo 0"]
    ).strip()
    assert audit_exists == "0", f"Audit log must be empty while disabled, got {audit_exists} lines"

    # Enable audit via config reload.
    node_reload.replace_in_config(
        AUDIT_LOG_CONFIG_PATH,
        "<allow_experimental_audit_log>false</allow_experimental_audit_log>",
        "<allow_experimental_audit_log>true</allow_experimental_audit_log>",
    )
    node_reload.query("SYSTEM RELOAD CONFIG")

    # Run a DDL after enabling — an audit record must appear.
    node_reload.query("CREATE TABLE test_reload_after(a int) ENGINE=Memory")
    assert_audit_log_contain_with_retry(node_reload, "test_reload_after")
    log_content = node_reload.grep_in_log("test_reload_after", from_host=True, filename="clickhouse-server.audit.log")
    assert "DDL" in log_content, "DDL audit record must appear after enabling via reload"

    # The DDL that ran while disabled must NOT appear.
    assert not node_reload.contains_in_log("test_reload_before", from_host=True, filename="clickhouse-server.audit.log")


def test_audit_log_reload_disable(start_cluster):
    """After enabling audit via reload, disabling it via another reload must
    stop emitting audit records immediately."""

    # Ensure audit is enabled (may already be from the previous test).
    node_reload.replace_in_config(
        AUDIT_LOG_CONFIG_PATH,
        "<allow_experimental_audit_log>false</allow_experimental_audit_log>",
        "<allow_experimental_audit_log>true</allow_experimental_audit_log>",
    )
    node_reload.query("SYSTEM RELOAD CONFIG")

    # Run a DDL and confirm audit is active.
    node_reload.query("DROP TABLE IF EXISTS test_disable_before")
    node_reload.query("CREATE TABLE test_disable_before(a int) ENGINE=Memory")
    assert_audit_log_contain_with_retry(node_reload, "test_disable_before")

    # Disable audit via config reload.
    node_reload.replace_in_config(
        AUDIT_LOG_CONFIG_PATH,
        "<allow_experimental_audit_log>true</allow_experimental_audit_log>",
        "<allow_experimental_audit_log>false</allow_experimental_audit_log>",
    )
    node_reload.query("SYSTEM RELOAD CONFIG")

    # Run a DDL after disabling — no new audit record should appear.
    node_reload.query("CREATE TABLE test_disable_after(a int) ENGINE=Memory")
    time.sleep(2)
    assert not node_reload.contains_in_log(
        "test_disable_after", from_host=True, filename="clickhouse-server.audit.log"
    ), "DDL after disabling audit must not appear in audit log"


def test_audit_log_failed_insert_select_object_names(start_cluster):
    """A multi-object statement that fails before execution starts must record every referenced
    object in OBJECT_NAMES, not only the top-level target. `INSERT INTO dst SELECT * FROM missing`
    fails while resolving the source table, before `logQueryStart` populates query_tables, so the
    names are extracted from the AST — and both the target and the missing source must be there."""
    dst = "audit_ins_sel_dst"
    missing = "audit_ins_sel_missing_src"
    node_dml_misc.query(f"DROP TABLE IF EXISTS {dst}")
    node_dml_misc.query(f"DROP TABLE IF EXISTS {missing}")
    node_dml_misc.query(f"CREATE TABLE {dst}(a Int32) ENGINE=Memory")

    error = node_dml_misc.query_and_get_error(f"INSERT INTO {dst} SELECT * FROM {missing}")
    assert error, "INSERT selecting from a missing table must fail"

    assert_audit_log_contain_with_retry(node_dml_misc, missing)
    log_content = node_dml_misc.grep_in_log(missing, from_host=True, filename="clickhouse-server.audit.log")
    lines = [line for line in log_content.strip().split("\n")
             if "AUDIT:" in line and "Insert" in line and missing in line]
    assert len(lines) >= 1, "A failed INSERT ... SELECT must produce a DML audit record"

    # Audit message format: "TYPE, COMMAND, EXCEPTION_CODE, USER, IP, OBJECT_NAMES, QUERY".
    # A literal comma inside a field (several object names) is escaped as '\,'.
    audit_part = lines[0].split("AUDIT: ", 1)[1]
    fields = re.split(r"(?<!\\), ", audit_part)
    assert fields[0] == "DML", f"INSERT must be classified as DML: {lines[0]}"
    assert fields[2] != "0", f"A failed INSERT must record a non-zero exception code: {lines[0]}"
    # OBJECT_NAMES is field index 5; it must carry the target AND the nested source table.
    assert dst in fields[5], f"OBJECT_NAMES must record the INSERT target table: {lines[0]}"
    assert missing in fields[5], f"OBJECT_NAMES must record the nested SELECT source table: {lines[0]}"

    node_dml_misc.query(f"DROP TABLE IF EXISTS {dst}")


def test_audit_log_reload_remove_sink(start_cluster):
    """Removing (emptying) `logger.auditlog` on a config reload must stop audit emission even
    though `allow_experimental_audit_log` stays enabled and a writer was already created. The
    writer object is intentionally kept alive (no teardown on reload), but no new statements may
    keep leaking into the old file path once the config no longer declares an audit sink."""

    audit_path = "/var/log/clickhouse-server/clickhouse-server.audit.log"

    # Ensure audit is enabled (the previous reload tests leave the flag disabled).
    node_reload.replace_in_config(
        AUDIT_LOG_CONFIG_PATH,
        "<allow_experimental_audit_log>false</allow_experimental_audit_log>",
        "<allow_experimental_audit_log>true</allow_experimental_audit_log>",
    )
    node_reload.query("SYSTEM RELOAD CONFIG")

    node_reload.query("DROP TABLE IF EXISTS test_remove_sink_before")
    node_reload.query("CREATE TABLE test_remove_sink_before(a int) ENGINE=Memory")
    assert_audit_log_contain_with_retry(node_reload, "test_remove_sink_before")

    # Remove the sink from the config while the feature flag stays enabled.
    node_reload.replace_in_config(
        AUDIT_LOG_CONFIG_PATH,
        f"<auditlog>{audit_path}</auditlog>",
        "<auditlog></auditlog>",
    )
    node_reload.query("SYSTEM RELOAD CONFIG")

    # New statements must stop appearing in the audit log.
    node_reload.query("CREATE TABLE test_remove_sink_after(a int) ENGINE=Memory")
    time.sleep(2)
    assert not node_reload.contains_in_log(
        "test_remove_sink_after", from_host=True, filename="clickhouse-server.audit.log"
    ), "DDL after removing logger.auditlog must not appear in the audit log"

    # Restore the config so later tests (and reruns) see the original state.
    node_reload.replace_in_config(
        AUDIT_LOG_CONFIG_PATH,
        "<auditlog></auditlog>",
        f"<auditlog>{audit_path}</auditlog>",
    )
    node_reload.replace_in_config(
        AUDIT_LOG_CONFIG_PATH,
        "<allow_experimental_audit_log>true</allow_experimental_audit_log>",
        "<allow_experimental_audit_log>false</allow_experimental_audit_log>",
    )
    node_reload.query("SYSTEM RELOAD CONFIG")
