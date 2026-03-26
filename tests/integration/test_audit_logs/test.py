import sys
import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node_ddl = cluster.add_instance(
    "node_audit_ddl",
    main_configs=[
        "configs/logger_audit_ddl.xml",
    ],
    stay_alive=True,
)
node_dml_misc = cluster.add_instance(
    "node_audit_dml_misc",
    main_configs=[
        "configs/logger_audit_dml_misc.xml",
    ],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_audit_log_filter(start_cluster):
    node_ddl.query("DROP TABLE IF EXISTS test_audit_log")
    node_ddl.query("CREATE TABLE test_audit_log(a int) ENGINE=Memory")
    node_ddl.query("USE default")
    node_ddl.query("INSERT INTO test_audit_log VALUES (0),(1),(2)")
    node_ddl.query("SELECT a FROM test_audit_log")

    # Audit DDL queries
    assert (
        int(
            node_ddl.exec_in_container(
                [
                    "bash",
                    "-c",
                    "grep 'DDL' /var/log/clickhouse-server/clickhouse-server.audit.log | wc -l",
                ],
                privileged=True,
                user="root",
            )
        )
        == 2
    )

    # Audit only DDL queries
    assert (
        int(
            node_ddl.exec_in_container(
                [
                    "bash",
                    "-c",
                    "cat /var/log/clickhouse-server/clickhouse-server.audit.log | wc -l",
                ],
                privileged=True,
                user="root",
            )
        )
        == 2
    )

    node_dml_misc.query("DROP TABLE IF EXISTS test_audit_log")
    node_dml_misc.query("CREATE TABLE test_audit_log(a int) ENGINE=Memory")
    node_dml_misc.query("USE default")
    node_dml_misc.query("INSERT INTO test_audit_log VALUES (0),(1),(2)")
    node_dml_misc.query("SELECT a FROM test_audit_log")

    # Audit DML and MISC queries
    assert (
        int(
            node_dml_misc.exec_in_container(
                [
                    "bash",
                    "-c",
                    "grep -E 'DML|MISC' /var/log/clickhouse-server/clickhouse-server.audit.log | wc -l",
                ],
                privileged=True,
                user="root",
            )
        )
        == 3
    )

    # Filter DDL queries
    assert (
        int(
            node_dml_misc.exec_in_container(
                [
                    "bash",
                    "-c",
                    "grep 'DDL' /var/log/clickhouse-server/clickhouse-server.audit.log | wc -l",
                ],
                privileged=True,
                user="root",
            )
        )
        == 0
    )

def test_audit_log_object_names(start_cluster):
    node_dml_misc.query("CREATE DATABASE IF NOT EXISTS test")
    node_dml_misc.query("DROP TABLE IF EXISTS test.t_audit")
    node_dml_misc.query("CREATE TABLE test.t_audit(a int) ENGINE=Memory")
    node_dml_misc.query("INSERT INTO test.t_audit VALUES (0),(1),(2)")

    assert (
        int(
            node_dml_misc.exec_in_container(
                [
                    "bash",
                    "-c",
                    "grep 'DML' /var/log/clickhouse-server/clickhouse-server.audit.log | grep 'test.t_audit' | wc -l",
                ],
                privileged=True,
                user="root",
            )
        )
        == 1
    )
