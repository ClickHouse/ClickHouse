# -*- coding: utf-8 -*-

import datetime
import decimal
import logging
import os
import uuid

import psycopg2 as py_psql
import psycopg2.extras
import pytest

from helpers.cluster import ClickHouseCluster, get_docker_compose_path, run_and_check

psycopg2.extras.register_uuid()

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
DOCKER_COMPOSE_PATH = get_docker_compose_path()

cluster = ClickHouseCluster(__file__)
cluster.add_instance(
    "node",
    main_configs=[
        "configs/postresql.xml",
        "configs/log.xml",
        "configs/ssl_conf.xml",
        "configs/dhparam.pem",
        "configs/server.crt",
        "configs/server.key",
    ],
    user_configs=["configs/default_passwd.xml"],
    with_postgres=True,
    with_postgresql_java_client=True,
    env_variables={"UBSAN_OPTIONS": "print_stacktrace=1"},
)

server_port = 5433


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        # Wait for the PostgreSQL handler to start.
        # Cluster.start waits until port 9000 becomes accessible.
        # Server opens the PostgreSQL compatibility port a bit later.
        cluster.instances["node"].wait_for_log_line("PostgreSQL compatibility protocol")
        yield cluster
    except Exception as ex:
        logging.exception(ex)
        raise ex
    finally:
        cluster.shutdown()


def test_psql_client(started_cluster):
    node = cluster.instances["node"]

    for query_file in ["query1.sql", "query2.sql", "query3.sql", "query4.sql"]:
        started_cluster.copy_file_to_container(
            started_cluster.postgres_id,
            os.path.join(SCRIPT_DIR, "queries", query_file),
            f"/{query_file}",
        )
    cmd_prefix = [
        "/usr/bin/psql",
        f"sslmode=require host={node.hostname} port={server_port} user=default dbname=default password=123",
    ]
    cmd_prefix += ["--no-align", "--field-separator=' '"]

    res = started_cluster.exec_in_container(
        started_cluster.postgres_id, cmd_prefix + ["-f", "/query1.sql"], shell=True
    )
    logging.debug(res)
    assert res == "\n".join(["a", "1", "(1 row)", ""])

    res = started_cluster.exec_in_container(
        started_cluster.postgres_id, cmd_prefix + ["-f", "/query2.sql"], shell=True
    )
    logging.debug(res)
    assert res == "\n".join(["a", "колонка", "(1 row)", ""])

    res = started_cluster.exec_in_container(
        started_cluster.postgres_id, cmd_prefix + ["-f", "/query3.sql"], shell=True
    )
    logging.debug(res)
    assert res == "\n".join(
        [
            "SELECT 0",
            "SELECT 0",
            "SELECT 0",
            "INSERT 0 0",
            "INSERT 0 0",
            "column",
            "0",
            "0",
            "1",
            "1",
            "5",
            "5",
            "(6 rows)",
            "SELECT 0\n",
        ]
    )

    res = started_cluster.exec_in_container(
        started_cluster.postgres_id, cmd_prefix + ["-f", "/query4.sql"], shell=True
    )
    logging.debug(res)
    assert res == "\n".join(
        ["SELECT 0", "INSERT 0 0", "tmp_column", "0", "1", "(2 rows)", "SELECT 0\n"]
    )


def test_python_client(started_cluster):
    node = cluster.instances["node"]

    with pytest.raises(py_psql.OperationalError) as exc_info:
        ch = py_psql.connect(
            host=node.ip_address,
            port=server_port,
            user="default",
            password="123",
            database="",
        )
        cur = ch.cursor()
        cur.execute("select name from tables;")

    assert exc_info.value.args == ("SSL connection has been closed unexpectedly\n",)

    ch = py_psql.connect(
        host=node.ip_address,
        port=server_port,
        user="default",
        password="123",
        database="",
    )
    cur = ch.cursor()

    cur.execute("select 1 as a, 2 as b")
    assert (cur.description[0].name, cur.description[1].name) == ("a", "b")
    assert cur.fetchall() == [(1, 2)]

    cur.execute("CREATE DATABASE x")
    cur.execute("USE x")
    cur.execute(
        "CREATE TEMPORARY TABLE tmp2 (ch Int8, i64 Int64, f64 Float64, str String, date Date, dec Decimal(19, 10), uuid UUID) ENGINE = Memory"
    )
    cur.execute(
        "insert into tmp2 (ch, i64, f64, str, date, dec, uuid) values (44, 534324234, 0.32423423, 'hello', '2019-01-23', 0.333333, '61f0c404-5cb3-11e7-907b-a6006ad3dba0')"
    )
    cur.execute("select * from tmp2")
    assert cur.fetchall()[0] == (
        "44",
        534324234,
        0.32423423,
        "hello",
        datetime.date(2019, 1, 23),
        decimal.Decimal("0.3333330000"),
        uuid.UUID("61f0c404-5cb3-11e7-907b-a6006ad3dba0"),
    )
    cur.execute("DROP DATABASE x")


def test_java_client(started_cluster):
    node = cluster.instances["node"]

    with open(os.path.join(SCRIPT_DIR, "java.reference")) as fp:
        reference = fp.read()

    # database not exists exception.
    with pytest.raises(Exception) as exc:
        res = started_cluster.exec_in_container(
            started_cluster.postgresql_java_client_docker_id,
            [
                "bash",
                "-c",
                f"java JavaConnectorTest --host {node.hostname} --port {server_port} --user default --database abc",
            ],
        )
        assert (
            "org.postgresql.util.PSQLException: ERROR: Invalid user or password"
            in str(exc.value)
        )

    # non-empty password passed.
    res = started_cluster.exec_in_container(
        started_cluster.postgresql_java_client_docker_id,
        [
            "bash",
            "-c",
            f"java JavaConnectorTest --host {node.hostname} --port {server_port} --user default --password 123 --database default",
        ],
    )
    assert res == reference
