import logging
import sys
import time

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
