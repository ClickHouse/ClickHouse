# -*- coding: utf-8 -*-

import datetime
import decimal
import logging
import os
import random
import socket
import struct
import threading
import time
import uuid
from io import StringIO

import psycopg
import psycopg2 as py_psql
import psycopg2.extras
import pytest

from helpers.cluster import ClickHouseCluster, get_docker_compose_path

psycopg2.extras.register_uuid()

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
DOCKER_COMPOSE_PATH = get_docker_compose_path()

cluster = ClickHouseCluster(__file__)
cluster.add_instance(
    "node",
    main_configs=[
        "configs/postgresql.xml",
        "configs/log.xml",
        "configs/ssl_conf.xml",
        "configs/dhparam.pem",
        "configs/server.crt",
        "configs/server.key",
    ],
    user_configs=[
        "configs/default_passwd.xml",
        "configs/sync_inserts.xml"
    ],
    with_postgres=True,
    with_postgresql_java_client=True,
    with_postgresql_dotnet_client=True,
)

cluster.add_instance(
    "node_secure",
    main_configs=[
        "configs/postgresql_secure.xml",
        "configs/log.xml",
        "configs/ssl_conf.xml",
        "configs/dhparam.pem",
        "configs/server.crt",
        "configs/server.key",
    ],
    user_configs=[
        "configs/default_passwd.xml",
        "configs/sync_inserts.xml"
    ],
    with_postgres=True,
    with_postgresql_java_client=True,
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

    for query_file in [
        "query1.sql",
        "query2.sql",
        "query3.sql",
        "query4.sql",
        "query5.sql",
        "query6.sql",
        "query7.sql",
    ]:
        started_cluster.copy_file_to_container(
            started_cluster.postgres_id,
            os.path.join(SCRIPT_DIR, "queries", query_file),
            f"/{query_file}",
        )
    cmd_prefix = [
        "/usr/bin/psql",
        f"sslmode=require host={node.hostname} port={server_port} user=user_with_sha256 dbname=default password=abacaba",
    ]
    # -F same as --field-separator
    cmd_prefix += ["--no-align", "-F", " "]

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
            "CREATE DATABASE",
            "USE",
            "CREATE TABLE",
            "INSERT 0 3",
            "INSERT 0 3",
            "column",
            "0",
            "0",
            "1",
            "1",
            "5",
            "5",
            "(6 rows)",
            "DROP DATABASE\n",
        ]
    )

    res = started_cluster.exec_in_container(
        started_cluster.postgres_id, cmd_prefix + ["-f", "/query4.sql"], shell=True
    )
    logging.debug(res)
    assert res == "\n".join(
        ["CREATE TABLE", "INSERT 0 2", "tmp_column", "0", "1", "(2 rows)", "DROP TABLE\n"]
    )

    res = started_cluster.exec_in_container(
        started_cluster.postgres_id, cmd_prefix + ["-f", "/query5.sql"], shell=True
    )
    logging.debug(res)
    assert res == "\n".join(
        [
            "CREATE DATABASE",
            "USE",
            "CREATE TABLE",
            "INSERT 0 3",
            "CREATE TABLE",
            "INSERT 0 3",
            "DROP DATABASE\n",
        ]
    )

    res = started_cluster.exec_in_container(
        started_cluster.postgres_id, cmd_prefix + ["-f", "/query6.sql"], shell=True
    )
    logging.debug(res)
    # PostgreSQL should return boolean values as 't' or 'f'
    assert res == "\n".join(
        ["bool_true bool_false", "t f", "(1 row)", ""]
    )

    res = started_cluster.exec_in_container(
        started_cluster.postgres_id, cmd_prefix + ["-f", "/query7.sql"], shell=True
    )
    logging.debug(res)
    # Test all DDL command tags
    assert res == "\n".join(
        [
            "CREATE DATABASE",
            "USE",
            "CREATE TABLE",
            "CREATE TABLE",
            "ALTER TABLE",
            "INSERT 0 3",
            "id name age",
            "1 Alice 25",
            "2 Bob 30",
            "3 Charlie 35",
            "(3 rows)",
            "SET",
            "TRUNCATE",
            "DROP TABLE",
            "DROP TABLE",
            "DROP DATABASE\n",
        ]
    )

def test_psql_client_secure(started_cluster):
    node = cluster.instances["node_secure"]

    started_cluster.copy_file_to_container(
        started_cluster.postgres_id,
        os.path.join(SCRIPT_DIR, "queries", "query1.sql"),
        "/query1.sql",
    )

    cmd_prefix = [
        "/usr/bin/psql",
        f"sslmode=require host={node.hostname} port={server_port} user=user_with_sha256 dbname=default password=abacaba",
    ]
    # -F same as --field-separator
    cmd_prefix += ["--no-align", "-F", " "]

    res = started_cluster.exec_in_container(
        started_cluster.postgres_id, cmd_prefix + ["-f", "/query1.sql"], shell=True
    )
    logging.debug(res)
    assert res == "\n".join(["a", "1", "(1 row)", ""])


    postgres_container = started_cluster.get_docker_handle(started_cluster.postgres_id);

    cmd_prefix = [
        "/usr/bin/psql",
        f"sslmode=disable host={node.hostname} port={server_port} user=user_with_sha256 dbname=default password=abacaba",
    ]
    # -F same as --field-separator
    cmd_prefix += ["--no-align", "-F", " "]

    code, (stdout, stderr) = postgres_container.exec_run(cmd_prefix + ["-f", "/query1.sql"], demux=True,)
    logging.debug(f"test_psql_client_secure code:{code} stdout:{stdout}, stderr:{stderr}")
    assert (
        "ERROR:  SSL connection required.\n"
        in stderr.decode()
    )

    assert node.contains_in_log(
        "<Error> PostgreSQLHandler: DB::Exception: SSL connection required."
    )


def test_new_user(started_cluster):
    node = cluster.instances["node"]

    db_id = f"x_{random.randint(0, 1000000)}"

    ch = py_psql.connect(
        host=node.ip_address,
        port=server_port,
        user="default",
        password="123",
        database="",
    )
    cur = ch.cursor()
    cur.execute(f"CREATE DATABASE {db_id}")
    cur.execute(f"USE {db_id}")
    cur.execute("CREATE USER IF NOT EXISTS name7 IDENTIFIED WITH scram_sha256_password BY 'my_password'")

    ch = py_psql.connect(
        host=node.ip_address,
        port=server_port,
        user="name7",
        password="my_password",
        database=db_id,
    )
    cur = ch.cursor()
    cur.execute("select 1;")
    assert cur.fetchall() == [(1,)]

    ch = py_psql.connect(
        host=node.ip_address,
        port=server_port,
        user="default",
        password="123",
        database="",
    )
    cur = ch.cursor()
    cur.execute(f"DROP DATABASE {db_id}")


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


def test_prepared_statement(started_cluster):
    node = started_cluster.instances["node"]

    ch = psycopg.connect(
        host=node.ip_address,
        port=server_port,
        user="default",
        password="123",
    )
    cur = ch.cursor()
    cur.execute("drop table if exists test;")

    cur.execute(
        """CREATE TABLE test(
            id INT
        ) ENGINE = Memory;"""
    )

    cur.execute("INSERT INTO test (id) VALUES (1), (2), (3);")

    cur.execute("SELECT * FROM test WHERE id > %s;", ('2',), prepare=True)
    assert cur.fetchall() == [(3,)]

    cur.execute("PREPARE select_test AS SELECT * FROM test WHERE id = $1;")
    cur.execute("EXECUTE select_test(1);")
    assert cur.fetchall() == [(1,)]

    cur.execute("DEALLOCATE select_test;")
    with pytest.raises(Exception):
        cur.execute("EXECUTE select_test(1);")


def test_copy_command(started_cluster):
    node = cluster.instances["node"]

    ch = py_psql.connect(
        host=node.ip_address,
        port=server_port,
        user="default",
        password="123",
        database="",
    )
    cur = ch.cursor()
    file_index = random.randint(0, 100000000)

    cur.execute("CREATE DATABASE copy_x")
    cur.execute("USE copy_x")

    cur.execute("drop table if exists test;")
    cur.execute("drop table if exists test_recreated;")

    # test copy to -> copy from cycle for simple table
    cur.execute("create table test (x UInt32) engine=Memory();")
    cur.execute("insert into test values (42),(43),(44),(45);")
    cur.execute("select * from test order by x;")
    assert cur.fetchall() == [(42,), (43,), (44,), (45,)]

    with open(f"out_{file_index}.tsv", "w") as f:
        cur.copy_to(file=f, table="test")
    with open(f"out_{file_index}.tsv", "r") as f:
        assert f.read() == "42\n43\n44\n45\n"

    cur.execute("create table test_recreated (x UInt32) engine=Memory();")
    data_to_copy = "1\n2\n3\n4\n5\n"
    cur.copy_from(StringIO(data_to_copy), "test_recreated", columns=("x",))
    cur.execute("select * from test_recreated order by x;")

    assert cur.fetchall() == [(1,), (2,), (3,), (4,), (5,)]

    cur.execute("drop table if exists test;")
    cur.execute("drop table if exists test_recreated;")

    # test copy to -> copy from cycle for complex table
    cur.execute("create table test (x UInt32, y String) engine=Memory();")
    cur.execute("insert into test values (42,'a'),(43,'b'),(44,'c'),(45,'d');")
    cur.execute("select * from test order by x;")

    assert cur.fetchall() == [(42, "a"), (43, "b"), (44, "c"), (45, "d")]

    with open(f"out_{file_index + 1}.tsv", "w") as f:
        cur.copy_to(file=f, table="test")
    with open(f"out_{file_index + 1}.tsv", "r") as f:
        assert f.read() == '42\ta\n43\tb\n44\tc\n45\td\n'

    cur.execute("create table test_recreated (x UInt32, y String) engine=Memory();")
    data_to_copy = "1\ta\n2\tb\n3\tc\n"
    cur.copy_from(StringIO(data_to_copy), "test_recreated", columns=("x","y"))
    cur.execute("select * from test_recreated order by x;")

    assert cur.fetchall() == [(1, "a"), (2, "b"), (3, "c")]
    cur.execute("DROP DATABASE copy_x")


def test_boolean_type(started_cluster):
    node = cluster.instances["node"]

    ch = py_psql.connect(
        host=node.ip_address,
        port=server_port,
        user="default",
        password="123",
        database="",
    )
    cur = ch.cursor()

    # Test boolean literals
    # PostgreSQL protocol MUST return boolean values as 't' or 'f' in text format
    # psycopg2 will automatically convert 't'/'f' to Python True/False
    # If server sends '1'/'0' or 'true'/'false', psycopg2 will NOT convert them to bool
    cur.execute("SELECT true AS bool_true, false AS bool_false")
    result = cur.fetchone()
    logging.debug(f"Boolean literals result: {result}, types: {type(result[0])}, {type(result[1])}")
    # psycopg2 should convert 't'/'f' to True/False automatically
    # If we get strings or numbers, it means the server didn't send proper PostgreSQL boolean format
    assert result == (True, False), \
        f"Expected (True, False) from psycopg2 conversion of 't'/'f', but got {result} with types {type(result[0])}, {type(result[1])}"

    # Test with table
    cur.execute("CREATE DATABASE test_bool_db")
    cur.execute("USE test_bool_db")
    cur.execute("CREATE TEMPORARY TABLE bool_test (id Int32, flag Bool) ENGINE = Memory")
    cur.execute("INSERT INTO bool_test VALUES (1, true), (2, false)")
    cur.execute("SELECT id, flag FROM bool_test ORDER BY id")
    results = cur.fetchall()
    logging.debug(f"Table boolean results: {results}")
    assert len(results) == 2
    # Strict check for boolean values from table
    assert results[0][1] is True, \
        f"Expected True (psycopg2 conversion of 't'), but got {results[0][1]} with type {type(results[0][1])}"
    assert results[1][1] is False, \
        f"Expected False (psycopg2 conversion of 'f'), but got {results[1][1]} with type {type(results[1][1])}"

    cur.execute("DROP TABLE bool_test")
    cur.execute("DROP DATABASE test_bool_db")
    cur.close()
    ch.close()


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


def test_dotnet_client(started_cluster):
    node = cluster.instances["node"]

    with open(os.path.join(SCRIPT_DIR, "dotnet.reference")) as fp:
        reference = fp.read()

    res = started_cluster.exec_in_container(
        started_cluster.postgresql_dotnet_client_docker_id,
        [
            "bash",
            "-c",
            f"cd /pg_testapp && dotnet run -- --host {node.hostname} --port {server_port} --username default --password 123",
        ],
    )
    # `dotnet run` builds first, so the .NET SDK can prepend build diagnostics to
    # stdout. That noise only appears before the client output, so tolerate it
    # with a directional suffix check while still catching any trailing or
    # inserted protocol divergence.
    assert res.endswith(reference)


def test_restricted_user_cannot_bypass_grants(started_cluster):
    """Verify that a user with limited grants can connect via PostgreSQL protocol
    (pg_type and other system views are initialized internally), but cannot
    perform operations beyond their granted privileges."""
    node = started_cluster.instances["node"]

    # Create a restricted user that can only SELECT from default database
    ch = psycopg.connect(
        host=node.ip_address,
        port=server_port,
        user="default",
        password="123",
    )
    cur = ch.cursor()
    cur.execute(
        "CREATE USER IF NOT EXISTS pg_restricted IDENTIFIED WITH plaintext_password BY 'restricted123'"
    )
    cur.execute("GRANT SELECT ON default.* TO pg_restricted")
    ch.close()

    # Connect as the restricted user - should succeed
    restricted = psycopg.connect(
        host=node.ip_address,
        port=server_port,
        user="pg_restricted",
        password="restricted123",
        dbname="default",
    )
    cur = restricted.cursor()

    # The internal pg_type view should be accessible.
    # ClickHouse currently sends scalar values over the PostgreSQL protocol in
    # text mode, so result[0] arrives as a string from psycopg.
    cur.execute("SELECT count() FROM pg_type")
    result = cur.fetchone()
    assert int(result[0]) > 0

    # SELECT should work
    cur.execute("SELECT 1")
    assert int(cur.fetchone()[0]) == 1

    # CREATE TABLE should be denied
    with pytest.raises(Exception) as exc:
        cur.execute("CREATE TABLE default.test_restricted (id Int32) ENGINE = Memory")
    assert "Not enough privileges" in str(exc.value)

    restricted.close()

    # Clean up
    ch = psycopg.connect(
        host=node.ip_address,
        port=server_port,
        user="default",
        password="123",
    )
    cur = ch.cursor()
    cur.execute("DROP USER IF EXISTS pg_restricted")
    ch.close()


def _assert_cancel_request_does_not_cancel_http_query(node, query_id, pid, key):
    """Runs a long HTTP query under `query_id`, sends an unauthenticated PostgreSQL CancelRequest
    naming (`pid`, `key`) while the query is running, and asserts the query is not affected."""
    result = {}

    def run_http_query():
        try:
            # The sleep must contribute to the returned value: a `sleepEachRow` column that no outer
            # expression consumes is dropped from the plan, and the query finishes instantly instead
            # of staying in the process list. One row per block gives the query one cancellation
            # checkpoint per 0.3 s for ~9 s, so a cancel that (wrongly) went through would reliably
            # fail it while the test is still watching.
            result["output"] = node.http_query(
                "SELECT sum(sleepEachRow(0.3) + number) FROM numbers(30)",
                params={"query_id": query_id, "max_block_size": "1"},
                user="default",
                password="123",
            )
        except Exception as e:
            result["error"] = str(e)

    thread = threading.Thread(target=run_http_query)
    thread.start()
    try:
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            # Poll over HTTP: a `clickhouse-client` round trip through `docker exec` can take
            # seconds under sanitizers, which would eat the window while the query is running.
            if (
                node.http_query(
                    f"SELECT count() FROM system.processes WHERE query_id = '{query_id}'",
                    user="default",
                    password="123",
                ).strip()
                == "1"
            ):
                break
            time.sleep(0.05)
        else:
            raise AssertionError(
                f"The HTTP query did not show up in the process list: {result}"
            )

        # An unauthenticated cancel request naming exactly that (process id, secret key) pair.
        with socket.create_connection((node.ip_address, server_port)) as sock:
            sock.sendall(struct.pack("!iiII", 16, 80877102, pid, key))
    finally:
        thread.join()

    assert result == {"output": "435\n"}


def test_cancel_request_does_not_cancel_foreign_query(started_cluster):
    """An unauthenticated PostgreSQL CancelRequest may only cancel queries that actually run on the
    PostgreSQL interface. The query id string alone is not a credential: any other interface lets a
    client pick an arbitrary query id, so a query that imitates the `postgres:<connection id>:<secret
    key>` shape must not be cancellable this way."""
    node = cluster.instances["node"]
    _assert_cancel_request_does_not_cancel_http_query(node, "postgres:1:2", 1, 2)


def test_cancel_request_does_not_cancel_query_reusing_freed_id(started_cluster):
    """Once a PostgreSQL connection is gone, its `postgres:<connection id>:<secret key>` query ids
    are free for any client to pick on another interface. A CancelRequest carrying that once-valid
    (process id, secret key) pair must not cancel the later query: the cancel must be bound to the
    exact query that was verified to run on the PostgreSQL interface, not to whatever currently
    holds the id."""
    node = cluster.instances["node"]

    # Run a statement over the PostgreSQL interface to obtain a genuinely server-assigned id, then
    # close the connection so the id is freed.
    ch = py_psql.connect(
        host=node.ip_address,
        port=server_port,
        user="default",
        password="123",
        database="",
    )
    cur = ch.cursor()
    cur.execute("SELECT 20250807")
    assert cur.fetchall() == [(20250807,)]
    ch.close()

    node.query("SYSTEM FLUSH LOGS query_log", password="123")
    query_id = node.query(
        "SELECT query_id FROM system.query_log"
        " WHERE query_id LIKE 'postgres:%' AND query LIKE 'SELECT 20250807%' AND type = 'QueryFinish'"
        " ORDER BY event_time_microseconds DESC LIMIT 1",
        password="123",
    ).strip()
    _, pid, key = query_id.split(":")
    _assert_cancel_request_does_not_cancel_http_query(node, query_id, int(pid), int(key))


def test_kill_query_cancels_paused_copy_from_stdin(started_cluster):
    """The staging loop of `COPY ... FROM STDIN` blocks on the client socket, so an external
    `KILL QUERY` (or `CancelRequest`) must be observed by polling: the insert has to leave the
    process list promptly even though the client is paused mid-copy and sends nothing, and the
    client gets `57014 query_canceled` once it finishes the copy. Nothing reaches the target
    table."""
    node = cluster.instances["node"]

    node.query(
        "CREATE TABLE copy_cancel_target (n UInt64) ENGINE = MergeTree ORDER BY n",
        password="123",
    )

    resume = threading.Event()
    result = {}

    class PausedPayload:
        """File-like COPY source: hands psycopg2 a first chunk, then blocks until the test has seen
        the query get killed, then reports end-of-data (which makes psycopg2 send `CopyDone` and
        collect the buffered error)."""

        def __init__(self):
            self.sent = False

        def read(self, size=-1):
            if not self.sent:
                self.sent = True
                return "1\n2\n"
            resume.wait(timeout=60)
            return ""

    def run_copy():
        ch = py_psql.connect(
            host=node.ip_address,
            port=server_port,
            user="default",
            password="123",
            database="",
        )
        try:
            cur = ch.cursor()
            cur.copy_expert("COPY copy_cancel_target FROM STDIN", PausedPayload())
            result["output"] = "copy completed"
        except Exception as e:
            result["error"] = str(e)
        finally:
            ch.close()

    def staged_copy_count():
        # Poll over HTTP: a `clickhouse-client` round trip through `docker exec` can take seconds
        # under sanitizers, which would blur the promptness this test is about.
        return node.http_query(
            "SELECT count() FROM system.processes"
            " WHERE query_id LIKE 'postgres:%' AND query LIKE '%copy_cancel_target%'",
            user="default",
            password="123",
        ).strip()

    thread = threading.Thread(target=run_copy)
    thread.start()
    try:
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if staged_copy_count() == "1":
                break
            time.sleep(0.05)
        else:
            raise AssertionError(
                f"The COPY insert did not show up in the process list: {result}"
            )

        node.query(
            "KILL QUERY WHERE query_id LIKE 'postgres:%'"
            " AND query LIKE '%copy_cancel_target%' ASYNC",
            password="123",
        )

        # The kill must take effect while the client stays paused: the query leaves the process
        # list on the staging loop's own cancellation check, not when the client next speaks.
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if staged_copy_count() == "0":
                break
            time.sleep(0.05)
        else:
            raise AssertionError(
                "The killed COPY stayed in the process list while the client was paused"
            )
    finally:
        resume.set()
        thread.join()

    assert "error" in result, f"the client did not see the cancellation: {result}"
    assert "cancel" in result["error"].lower(), result["error"]
    assert (
        node.query(
            "SELECT count() FROM copy_cancel_target", password="123"
        ).strip()
        == "0"
    )
    node.query("DROP TABLE copy_cancel_target SYNC", password="123")


def test_array_type_oids_in_row_description(started_cluster):
    """A `SELECT` over the PostgreSQL wire must advertise array columns with the array OID of their
    element type in `RowDescription`, matching the emulated `pg_attribute`/`pg_type` catalog, so that
    clients decode the `{...}` payload as an array rather than a string. `DateTime` arrays take the
    `text[]` fallback, like the scalar type takes `text`."""
    node = cluster.instances["node"]

    ch = py_psql.connect(
        host=node.ip_address,
        port=server_port,
        user="default",
        password="123",
        database="",
    )
    cur = ch.cursor()
    cur.execute(
        "SELECT CAST([1, 2, 3], 'Array(Int32)') AS int4_arr,"
        " CAST(['a', 'b'], 'Array(String)') AS text_arr,"
        " CAST([[1], [2]], 'Array(Array(Int64))') AS int8_arr,"
        " CAST([1.5], 'Array(Float64)') AS float8_arr,"
        " CAST([toDateTime('2020-01-02 03:04:05', 'UTC')], 'Array(DateTime(\\'UTC\\'))') AS datetime_arr"
    )
    assert [d.type_code for d in cur.description] == [1007, 1009, 1016, 1022, 1009]
    assert cur.fetchall() == [
        (
            [1, 2, 3],
            ["a", "b"],
            [[1], [2]],
            [1.5],
            ["2020-01-02 03:04:05"],
        )
    ]
    ch.close()
