# -*- coding: utf-8 -*-

import base64
import datetime
import decimal
import hashlib
import logging
import os
import random
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


def test_psql_describe(started_cluster):
    node = cluster.instances["node"]

    started_cluster.copy_file_to_container(
        started_cluster.postgres_id,
        os.path.join(SCRIPT_DIR, "queries", "query8.sql"),
        "/query8.sql",
    )

    cmd_prefix = [
        "/usr/bin/psql",
        f"sslmode=require host={node.hostname} port={server_port} user=user_with_sha256 dbname=default password=abacaba",
    ]
    # -F same as --field-separator
    cmd_prefix += ["--no-align", "-F", " "]

    res = started_cluster.exec_in_container(
        started_cluster.postgres_id, cmd_prefix + ["-f", "/query8.sql"], shell=True
    )
    logging.debug(res)
    # \d lists the tables of the current database (with their types and owner),
    # \dt lists only the tables. The exact psql chrome (headers, row counts)
    # varies between psql versions, so check the rows themselves.
    assert "db_psql_describe t_described table user_with_sha256" in res
    assert "db_psql_describe v_described view user_with_sha256" in res
    # The view must not be listed by \dt: expect exactly one more listing of the
    # table (from \dt) than of the view (only from \d).
    assert res.count("t_described table") == 2
    assert res.count("v_described view") == 1


def test_query_error_keeps_connection(started_cluster):
    node = cluster.instances["node"]

    ch = py_psql.connect(
        host=node.ip_address,
        port=server_port,
        user="default",
        password="123",
        dbname="default",
    )
    cur = ch.cursor()

    # A failed query must not tear the connection down: the server sends
    # ErrorResponse and returns to the ReadyForQuery state, like PostgreSQL.
    with pytest.raises(Exception) as exc:
        cur.execute("SELECT this is not valid SQL")
    assert "Query execution failed" in str(exc.value)

    cur.execute("SELECT 1")
    assert int(cur.fetchone()[0]) == 1

    # Same for an error from query execution (not parsing).
    with pytest.raises(Exception) as exc:
        cur.execute("SELECT throwIf(1)")

    cur.execute("SELECT 2")
    assert int(cur.fetchone()[0]) == 2

    ch.close()


def test_prepared_query_error_keeps_connection(started_cluster):
    node = cluster.instances["node"]

    ch = psycopg.connect(
        host=node.ip_address,
        port=server_port,
        user="default",
        password="123",
        dbname="default",
    )
    cur = ch.cursor()

    # An error in the extended protocol keeps the connection usable after its
    # `Sync`, which psycopg sends before reporting the failed operation.
    with pytest.raises(Exception):
        cur.execute("SELECT throwIf(1)", prepare=True)

    cur.execute("SELECT 1", prepare=True)
    assert int(cur.fetchone()[0]) == 1
    ch.close()


def test_prepared_query_error_after_output_closes_connection(started_cluster):
    node = cluster.instances["node"]

    ch = psycopg.connect(
        host=node.ip_address,
        port=server_port,
        user="default",
        password="123",
        dbname="default",
    )
    cur = ch.cursor()

    # When an extended-protocol `Execute` fails after some result bytes were
    # already sent, the output stream may be cut in the middle of a protocol
    # message, so the server must tear the connection down instead of
    # returning to the `ReadyForQuery` state.
    with pytest.raises(Exception):
        cur.execute(
            "SELECT throwIf(number = 100000) FROM numbers(1000000)", prepare=True
        )

    with pytest.raises(Exception):
        cur.execute("SELECT 1", prepare=True)

    ch.close()


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


def test_scram_user_with_multiple_auth_methods(started_cluster):
    # A user that has a non-password authentication method (e.g. ssh_key) in addition to
    # scram_sha256_password must still be able to authenticate over the PostgreSQL protocol
    # with the password, regardless of the order of the methods.
    node = cluster.instances["node"]

    ssh_key = "AAAAC3NzaC1lZDI1NTE5AAAAIAKI0BUOuCJvCglpUyvIuJhF3cOlzzVcG53LTOHznXYL"

    # Two live verifiers that share one explicit salt are representable on the wire: the salt sent in
    # `AuthenticationSASLContinue` is the same for both, and the client proof is checked against every
    # stored salted password of the user.
    shared_salt = "c2FsdHNhbHRzYWx0c2FsdA=="
    shared_salt_hashes = [
        hashlib.pbkdf2_hmac(
            "sha256", password.encode(), base64.b64decode(shared_salt), 4096
        ).hex()
        for password in ("p123", "other_password")
    ]

    # The same password stored twice as `scram_sha256_password` gets two different random salts. Only one of them can
    # be sent on the wire, so the fail-close ambiguity scan of the access layer cannot match the other method and the
    # configuration must be refused instead of logging in with weaker checks than the native protocol.
    expired_shared_salt_hash = hashlib.pbkdf2_hmac(
        "sha256", b"p123", base64.b64decode(shared_salt), 4096
    ).hex()

    users = {
        "user_scram_then_ssh": f"scram_sha256_password BY 'p123', ssh_key BY KEY '{ssh_key}' TYPE 'ssh-ed25519'",
        "user_ssh_then_scram": f"ssh_key BY KEY '{ssh_key}' TYPE 'ssh-ed25519', scram_sha256_password BY 'p123'",
        "user_scram_then_sha256": "scram_sha256_password BY 'p123', sha256_password BY 'other_password'",
        "user_two_scram": "scram_sha256_password BY 'p123', scram_sha256_password BY 'other_password'",
        "user_plaintext_and_two_scram": "plaintext_password BY 'p123', scram_sha256_password BY 'p123', scram_sha256_password BY 'other_password'",
        "user_expired_then_live_scram": "scram_sha256_password BY 'expired' VALID UNTIL '2010-01-01', scram_sha256_password BY 'p123'",
        "user_shared_password_expired_scram": "scram_sha256_password BY 'p123' VALID UNTIL '2010-01-01', scram_sha256_password BY 'p123'",
        "user_shared_password_limited_scram": "scram_sha256_password BY 'p123' GRANTS (SELECT ON system.numbers), scram_sha256_password BY 'p123'",
        "user_expired_only_scram": "scram_sha256_password BY 'p123' VALID UNTIL '2010-01-01'",
        "user_plaintext_and_expired_scram": "plaintext_password BY 'p123', scram_sha256_password BY 'old' VALID UNTIL '2010-01-01'",
        "user_two_scram_same_salt": (
            f"scram_sha256_hash BY '{shared_salt_hashes[0]}' SALT '{shared_salt}', "
            f"scram_sha256_hash BY '{shared_salt_hashes[1]}' SALT '{shared_salt}'"
        ),
        "user_expired_and_live_same_salt": (
            f"scram_sha256_hash BY '{expired_shared_salt_hash}' SALT '{shared_salt}' VALID UNTIL '2010-01-01', "
            f"scram_sha256_hash BY '{expired_shared_salt_hash}' SALT '{shared_salt}'"
        ),
    }

    # PostgreSQL SCRAM cannot represent these configurations: either it cannot choose between the salts of several
    # live verifiers, or a method that would narrow the session (`VALID UNTIL`, `GRANTS`) cannot be matched by a
    # client proof bound to the salt that is sent on the wire.
    unsupported_configuration_users = {
        "user_two_scram",
        "user_expired_then_live_scram",
        "user_shared_password_expired_scram",
        "user_shared_password_limited_scram",
    }

    # The exchange runs, but no method can accept the credential: the only verifier has expired, or the shared salt
    # lets the fail-close scan match the expired method and expire the whole login.
    invalid_credentials_users = {
        "user_expired_only_scram",
        "user_expired_and_live_same_salt",
    }

    try:
        for name, methods in users.items():
            node.query(f"CREATE USER {name} IDENTIFIED WITH {methods}", password="123")
            node.query(f"GRANT SELECT ON system.one TO {name}", password="123")

            if name in unsupported_configuration_users:
                with pytest.raises(py_psql.OperationalError, match="Authentication configuration is not supported"):
                    py_psql.connect(
                        host=node.ip_address,
                        port=server_port,
                        user=name,
                        password="p123",
                        database="system",
                    )
                continue

            if name in invalid_credentials_users:
                with pytest.raises(py_psql.OperationalError, match="Invalid user or password"):
                    py_psql.connect(
                        host=node.ip_address,
                        port=server_port,
                        user=name,
                        password="p123",
                        database="system",
                    )
                continue

            ch = py_psql.connect(
                host=node.ip_address,
                port=server_port,
                user=name,
                password="p123",
                database="system",
            )
            cur = ch.cursor()
            cur.execute("SELECT 1;")
            assert cur.fetchall() == [(1,)]
            ch.close()

            with pytest.raises(py_psql.OperationalError):
                py_psql.connect(
                    host=node.ip_address,
                    port=server_port,
                    user=name,
                    password="wrong_password",
                    database="system",
                )

        with pytest.raises(py_psql.OperationalError, match="Authentication configuration is not supported"):
            py_psql.connect(
                host=node.ip_address,
                port=server_port,
                user="user_with_scram_and_otp",
                password="abacaba",
                database="system",
            )
    finally:
        for name in users:
            node.query(f"DROP USER IF EXISTS {name}", password="123")


def test_python_client(started_cluster):
    node = cluster.instances["node"]

    ch = py_psql.connect(
        host=node.ip_address,
        port=server_port,
        user="default",
        password="123",
        database="",
    )
    cur = ch.cursor()

    # A failed query returns an error and keeps the connection usable
    # (as in PostgreSQL) instead of closing the connection.
    with pytest.raises(py_psql.errors.SqlRoutineException) as exc_info:
        cur.execute("select name from tables;")

    assert "Unknown table expression identifier" in str(exc_info.value)

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

    # The internal compatibility views should be accessible without direct
    # grants on their `system.*` sources.
    # ClickHouse currently sends scalar values over the PostgreSQL protocol in
    # text mode, so result[0] arrives as a string from psycopg.
    cur.execute("SELECT count() FROM pg_type")
    result = cur.fetchone()
    assert int(result[0]) > 0

    cur.execute("SELECT count() FROM pg_namespace")
    assert int(cur.fetchone()[0]) > 0

    cur.execute("SELECT count() FROM pg_class")
    assert int(cur.fetchone()[0]) > 0

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


def test_restricted_user_catalog_visibility(started_cluster):
    """The pg_namespace / pg_class compatibility views must expose only the
    metadata visible to the session user: a user granted a single table must
    not be able to enumerate other databases or ungranted tables through
    them."""
    node = started_cluster.instances["node"]

    ch = psycopg.connect(
        host=node.ip_address,
        port=server_port,
        user="default",
        password="123",
    )
    cur = ch.cursor()
    cur.execute("CREATE DATABASE IF NOT EXISTS pg_visible_db")
    cur.execute("CREATE DATABASE IF NOT EXISTS pg_hidden_db")
    cur.execute(
        "CREATE TABLE IF NOT EXISTS pg_visible_db.t_granted (id Int32) ENGINE = Memory"
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS pg_visible_db.t_ungranted (id Int32) ENGINE = Memory"
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS pg_hidden_db.t_hidden (id Int32) ENGINE = Memory"
    )
    cur.execute(
        "CREATE USER IF NOT EXISTS pg_narrow IDENTIFIED WITH plaintext_password BY 'narrow123'"
    )
    cur.execute("GRANT SELECT ON pg_visible_db.t_granted TO pg_narrow")
    ch.close()

    narrow = psycopg.connect(
        host=node.ip_address,
        port=server_port,
        user="pg_narrow",
        password="narrow123",
        dbname="pg_visible_db",
    )
    cur = narrow.cursor()

    # pg_namespace must list the granted database but not unrelated ones.
    cur.execute("SELECT nspname FROM pg_namespace")
    namespaces = {row[0] for row in cur.fetchall()}
    assert "pg_visible_db" in namespaces
    assert "pg_hidden_db" not in namespaces

    # pg_class (behind psql's \d) must list only the granted table.
    cur.execute("SELECT relname FROM pg_class WHERE relname != ''")
    relations = {row[0] for row in cur.fetchall()}
    assert "t_granted" in relations
    assert "t_ungranted" not in relations
    assert "t_hidden" not in relations

    narrow.close()

    # Clean up
    ch = psycopg.connect(
        host=node.ip_address,
        port=server_port,
        user="default",
        password="123",
    )
    cur = ch.cursor()
    cur.execute("DROP USER IF EXISTS pg_narrow")
    cur.execute("DROP DATABASE IF EXISTS pg_visible_db")
    cur.execute("DROP DATABASE IF EXISTS pg_hidden_db")
    ch.close()
