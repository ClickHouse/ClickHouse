# -*- coding: utf-8 -*-

import datetime
import decimal
import logging
import os
import random
import uuid
from contextlib import closing
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


def test_prepared_statement_no_sql_injection(started_cluster):
    # Bound parameters must be treated as data, never spliced into the SQL text.
    # A parameter such as "x' UNION ALL SELECT ..." must not be able to break out
    # of the literal and read another table.
    node = started_cluster.instances["node"]

    ch = psycopg.connect(
        host=node.ip_address,
        port=server_port,
        user="default",
        password="123",
    )
    cur = ch.cursor()
    cur.execute("DROP TABLE IF EXISTS inj_users;")
    cur.execute("DROP TABLE IF EXISTS inj_secret;")
    cur.execute("CREATE TABLE inj_users (id Int32, name String) ENGINE = Memory;")
    cur.execute("INSERT INTO inj_users (id, name) VALUES (1, 'alice'), (2, 'bob');")
    cur.execute("CREATE TABLE inj_secret (sid Int32, secret String) ENGINE = Memory;")
    cur.execute("INSERT INTO inj_secret (sid, secret) VALUES (99, 'TOP_SECRET');")

    # A parameterized execute already uses the extended Parse/Bind/Execute path
    # (the bound value travels the wire as data) - that is the path under test.
    # We do not pass prepare=True: it adds a named, server-side cached statement
    # whose lifecycle is driven by the client's own prepared-statement cache, and
    # repeated named prepares on one connection are an unrelated source of
    # flakiness here. The unnamed parameterized form exercises the same binding.

    # Benign string parameter.
    cur.execute("SELECT id FROM inj_users WHERE name = %s;", ("bob",))
    assert cur.fetchall() == [(2,)]

    # Numeric comparison with a (text) parameter still works.
    cur.execute("SELECT id FROM inj_users WHERE id > %s ORDER BY id;", ("1",))
    assert cur.fetchall() == [(2,)]

    # Injection attempt: the payload must be bound as a single string literal,
    # not interpreted as SQL, so the secret table is never read.
    payload = "x' UNION ALL SELECT secret FROM inj_secret -- "
    cur.execute("SELECT name FROM inj_users WHERE name = %s;", (payload,))
    assert cur.fetchall() == []

    # Placeholder inside a block comment: $1 there is not a real placeholder, so
    # the bound value must not be spliced into the comment. Otherwise a value
    # beginning with "*/ ... --" closes the comment and what follows becomes
    # executable SQL ahead of the real placeholder. The body keeps a $1 in a
    # leading comment and a real $1 in the WHERE; the secret must stay unread
    # (pre-fix this leaked TOP_SECRET).
    payload = "*/ SELECT secret FROM inj_secret -- "
    cur.execute("/* $1 */ SELECT name FROM inj_users WHERE name = %s;", (payload,))
    assert ("TOP_SECRET",) not in cur.fetchall()

    # A parameter with a single quote must round-trip as data.
    cur.execute("SELECT %s AS v;", ("O'Brien",))
    assert cur.fetchall() == [("O'Brien",)]

    cur.execute("DROP TABLE inj_users;")
    cur.execute("DROP TABLE inj_secret;")


def test_execute_no_sql_injection(started_cluster):
    # Simple-query PREPARE/EXECUTE path: EXECUTE arguments are spliced into the
    # prepared statement body by $N substitution, so a string argument must be
    # emitted as a quoted+escaped SQL literal, never as raw SQL text.
    node = started_cluster.instances["node"]

    ch = psycopg.connect(
        host=node.ip_address,
        port=server_port,
        user="default",
        password="123",
    )
    cur = ch.cursor()
    cur.execute("DROP TABLE IF EXISTS exec_users;")
    cur.execute("DROP TABLE IF EXISTS exec_secret;")
    cur.execute("CREATE TABLE exec_users (id Int32, name String) ENGINE = Memory;")
    cur.execute("INSERT INTO exec_users (id, name) VALUES (1, 'alice'), (2, 'bob');")
    cur.execute("CREATE TABLE exec_secret (sid Int32, secret String) ENGINE = Memory;")
    cur.execute("INSERT INTO exec_secret (sid, secret) VALUES (99, 'TOP_SECRET');")

    # Numeric argument: stays a bare number, normal lookup works.
    cur.execute("PREPARE by_id AS SELECT name FROM exec_users WHERE id = $1;")
    cur.execute("EXECUTE by_id(2);")
    assert cur.fetchall() == [("bob",)]

    # String argument: must be treated as a single literal, normal lookup works.
    cur.execute("PREPARE by_name AS SELECT id FROM exec_users WHERE name = $1;")
    cur.execute("EXECUTE by_name('alice');")
    assert cur.fetchall() == [(1,)]

    # The vulnerable shape: the prepared body wraps the placeholder in quotes
    # (WHERE name = '$1'), exactly how a client expects to pass a string. The
    # argument must be escaped before substitution; otherwise a doubled quote
    # closes the literal and the UNION leaks the secret table.
    cur.execute("PREPARE by_name_q AS SELECT id FROM exec_users WHERE name = '$1';")
    cur.execute(
        "EXECUTE by_name_q('bob'' UNION ALL SELECT secret FROM exec_secret -- ');"
    )
    rows = cur.fetchall()
    assert ("TOP_SECRET",) not in rows

    # An argument containing a single quote round-trips as data.
    cur.execute("PREPARE echo_one AS SELECT $1 AS v;")
    cur.execute("EXECUTE echo_one('O''Brien');")
    assert cur.fetchall() == [("O'Brien",)]

    cur.execute("DEALLOCATE by_id;")
    cur.execute("DEALLOCATE by_name;")
    cur.execute("DEALLOCATE by_name_q;")
    cur.execute("DEALLOCATE echo_one;")
    cur.execute("DROP TABLE exec_users;")
    cur.execute("DROP TABLE exec_secret;")


def test_copy_no_sql_injection(started_cluster):
    # COPY builds its SELECT/INSERT from the client-supplied table and column
    # identifiers. A malicious identifier (quoted so it survives as a single
    # token) must be back-quoted into one harmless identifier, never spliced as
    # raw SQL, so it cannot break out into a UNION or a second statement.
    node = started_cluster.instances["node"]

    def connect():
        # psycopg2's `with connection` manages the transaction but does NOT close
        # the connection, so wrap in closing() to guarantee each probe's broken
        # connection is actually closed.
        c = py_psql.connect(
            host=node.ip_address,
            port=server_port,
            user="default",
            password="123",
            database="",
        )
        c.autocommit = True
        return closing(c)

    setup = py_psql.connect(
        host=node.ip_address,
        port=server_port,
        user="default",
        password="123",
        database="",
    )
    setup.autocommit = True
    setup_cur = setup.cursor()
    setup_cur.execute("DROP TABLE IF EXISTS copy_t;")
    setup_cur.execute("DROP TABLE IF EXISTS copy_secret;")
    setup_cur.execute("CREATE TABLE copy_t (x UInt32) ENGINE = Memory;")
    setup_cur.execute("INSERT INTO copy_t VALUES (1), (2);")
    setup_cur.execute("CREATE TABLE copy_secret (s String) ENGINE = Memory;")
    setup_cur.execute("INSERT INTO copy_secret VALUES ('TOP_SECRET');")
    setup_cur.execute("DROP TABLE IF EXISTS copy_load;")
    setup_cur.execute("CREATE TABLE copy_load (s String) ENGINE = Memory;")
    setup_cur.execute("DROP TABLE IF EXISTS copy_secret_str;")
    setup_cur.execute("CREATE TABLE copy_secret_str (s String) ENGINE = Memory;")
    setup_cur.execute("INSERT INTO copy_secret_str VALUES ('TOP_SECRET');")

    # A COPY rejected by the server leaves the psycopg2 connection in a broken
    # state (the next call raises "cursor already closed"), so every COPY attempt
    # below uses its own connection. Otherwise the benign COPY after a blocked
    # one would fail for a reason unrelated to the security check under test.

    # Malicious table identifier: the whole UNION is wrapped in one quoted
    # identifier so it reaches the handler as a single name. It must be treated
    # as one (non-existent) table name, not executed as SQL.
    out = StringIO()
    with connect() as c, pytest.raises(Exception):
        c.cursor().copy_expert(
            'COPY "copy_t UNION ALL SELECT s FROM copy_secret" TO STDOUT', out
        )
    assert "TOP_SECRET" not in out.getvalue()

    # Malicious column identifier: same idea via the column list.
    out2 = StringIO()
    with connect() as c, pytest.raises(Exception):
        c.cursor().copy_expert(
            'COPY copy_t ("x) , (SELECT s FROM copy_secret") TO STDOUT', out2
        )
    assert "TOP_SECRET" not in out2.getvalue()

    # A benign COPY on a fresh connection still works.
    out3 = StringIO()
    with connect() as c:
        c.cursor().copy_expert("COPY copy_t TO STDOUT", out3)
    assert sorted(out3.getvalue().split()) == ["1", "2"]

    # COPY FROM builds an INSERT INTO from the same client-supplied identifiers.
    # A malicious column identifier (quoted so it reaches the handler as one
    # token) must be back-quoted into a single column name. Otherwise it is
    # spliced raw and turns the INSERT into "INSERT INTO load (s) SELECT s FROM
    # secret", copying the secret into the load table.
    with connect() as c, pytest.raises(Exception):
        c.cursor().copy_expert(
            'COPY copy_load ("s) SELECT s FROM copy_secret_str -- ") FROM STDIN',
            StringIO("x\n"),
        )
    # The injected SELECT must not have run: the load table stays empty.
    # ClickHouse returns the count over the PostgreSQL wire as text, so cast it.
    setup_cur.execute("SELECT count() FROM copy_load WHERE s = 'TOP_SECRET';")
    assert int(setup_cur.fetchone()[0]) == 0

    # A benign COPY FROM with a legitimate column still works.
    with connect() as c:
        c.cursor().copy_expert("COPY copy_load (s) FROM STDIN", StringIO("hello\n"))
    setup_cur.execute("SELECT s FROM copy_load;")
    assert setup_cur.fetchall() == [("hello",)]

    setup_cur.execute("DROP TABLE copy_load;")
    setup_cur.execute("DROP TABLE copy_secret_str;")
    setup_cur.execute("DROP TABLE copy_t;")
    setup_cur.execute("DROP TABLE copy_secret;")
    setup.close()


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
