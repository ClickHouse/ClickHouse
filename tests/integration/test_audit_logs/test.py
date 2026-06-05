import logging
import socket
import struct
import sys
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

    assert_audit_log_contain_with_retry(node_dml_misc, "DML")
    log_content = node_dml_misc.grep_in_log("DML", from_host=True, filename="clickhouse-server.audit.log")
    assert "test.t_audit" in log_content


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


def test_audit_log_pg_unsupported_auth_method(start_cluster):
    """A user whose only auth method is unsupported by the PostgreSQL protocol
    must still emit a LoginFailure audit event with the attempted user and client IP."""
    # Create a user that only supports double_sha1_password (not supported by the PG handler)
    node_user_dcl.query(
        "CREATE USER IF NOT EXISTS pg_unsupported_auth_user IDENTIFIED WITH double_sha1_password BY 'secret123'"
    )

    # Send a raw PostgreSQL startup message to trigger the auth-method negotiation path
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5)
        s.connect((node_user_dcl.ip_address, 9005))
        s.sendall(pg_startup_message("pg_unsupported_auth_user"))
        # Read response (we expect an error)
        s.recv(4096)
    except Exception:
        pass
    finally:
        s.close()

    assert_audit_log_contain_with_retry(node_user_dcl, "pg_unsupported_auth_user")
    log_content = node_user_dcl.grep_in_log("pg_unsupported_auth_user", from_host=True, filename="clickhouse-server.audit.log")
    assert "LoginFailure" in log_content, "LoginFailure must be emitted for unsupported auth-method negotiation"
    assert "Unknown Host" not in log_content, "Client IP must be recorded, not 'Unknown Host'"


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


def test_audit_log_mysql_ssl_required_failure(start_cluster):
    """When MySQL requires SSL but client doesn't support it, must emit LoginFailure with client IP."""
    unique_user = "mysql_ssl_req_audit_user"

    # Create a config that requires secure transport for MySQL
    mysql_secure_config = """<clickhouse>
    <mysql_port>9004</mysql_port>
    <mysql_require_secure_transport>true</mysql_require_secure_transport>
</clickhouse>
"""
    node_user_dcl.exec_in_container(
        ["bash", "-c", f"echo '{mysql_secure_config}' > /etc/clickhouse-server/config.d/mysql_secure.xml"]
    )
    node_user_dcl.query("SYSTEM RELOAD CONFIG")

    # Wait for reload to take effect
    time.sleep(1)

    s = None
    try:
        # Connect to MySQL port and complete the initial handshake exchange
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5)
        s.connect((node_user_dcl.ip_address, 9004))
        # Read server's Handshake packet first
        s.recv(4096)
        # Send a valid HandshakeResponse without CLIENT_SSL flag
        s.sendall(mysql_handshake_response(unique_user))
        # Read error response
        s.recv(4096)
    except Exception:
        pass  # Expected to fail
    finally:
        try:
            if s:
                s.close()
        except Exception:
            pass
        # Clean up config
        node_user_dcl.exec_in_container(
            ["bash", "-c", "rm -f /etc/clickhouse-server/config.d/mysql_secure.xml"]
        )
        node_user_dcl.query("SYSTEM RELOAD CONFIG")

    # Verify LoginFailure was logged with the unique username and client IP
    assert_audit_log_contain_with_retry(node_user_dcl, unique_user)
    log_content = node_user_dcl.grep_in_log(unique_user, from_host=True, filename="clickhouse-server.audit.log")
    assert "LoginFailure" in log_content, "LoginFailure must be emitted for SSL-required failure"
    assert "Unknown Host" not in log_content, "Client IP must be recorded for SSL-required failure, not 'Unknown Host'"


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
