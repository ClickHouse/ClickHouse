# -*- coding: utf-8 -*-

import base64
import datetime
import decimal
import hashlib
import logging
import os
import random
import socket
import struct
import threading
import time
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
        "configs/postgresql_alt_protocol.xml",
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
# The second PostgreSQL endpoint from `configs/postgresql_alt_protocol.xml`, served by its own listener.
alt_server_port = 5435


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


def test_prepared_statement_no_sql_injection(started_cluster):
    # Bound parameters must remain data, never SQL.
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

    # The unnamed parameterized form exercises `Parse`/`Bind`/`Execute` directly.

    # Benign string parameter.
    cur.execute("SELECT id FROM inj_users WHERE name = %s;", ("bob",))
    assert cur.fetchall() == [(2,)]

    # Numeric comparison with a (text) parameter still works.
    cur.execute("SELECT id FROM inj_users WHERE id > %s ORDER BY id;", ("1",))
    assert cur.fetchall() == [(2,)]

    # The injection payload remains one string value.
    payload = "x' UNION ALL SELECT secret FROM inj_secret -- "
    cur.execute("SELECT name FROM inj_users WHERE name = %s;", (payload,))
    assert cur.fetchall() == []

    # Ignore placeholders inside comments; substitute only the real `$1`.
    payload = "*/ SELECT secret FROM inj_secret -- "
    cur.execute("/* $1 */ SELECT name FROM inj_users WHERE name = %s;", (payload,))
    assert ("TOP_SECRET",) not in cur.fetchall()

    # A parameter with a single quote must round-trip as data.
    cur.execute("SELECT %s AS v;", ("O'Brien",))
    assert cur.fetchall() == [("O'Brien",)]

    cur.execute("DROP TABLE inj_users;")
    cur.execute("DROP TABLE inj_secret;")


def test_bind_binary_format_rejected(started_cluster):
    # Reject binary `Bind` values because the handler implements only text decoding.
    node = started_cluster.instances["node"]

    ch = psycopg.connect(
        host=node.ip_address,
        port=server_port,
        user="default",
        password="123",
    )
    pg = ch.pgconn

    # Sanity: the same query with a text format code (0) is accepted.
    res_text = pg.exec_params(
        b"SELECT $1::Int32", [b"42"], None, [0], 0
    )
    assert res_text.status == psycopg.pq.ExecStatus.TUPLES_OK, (
        res_text.error_message
    )
    assert res_text.get_value(0, 0) == b"42"

    # Binary format code (1) must be rejected, not silently misbound.
    res_bin = pg.exec_params(
        b"SELECT $1::Int32", [b"\x00\x00\x00\x2a"], None, [1], 0
    )
    assert res_bin.status == psycopg.pq.ExecStatus.FATAL_ERROR
    assert b"Binary format parameters are not supported" in res_bin.error_message

    ch.close()


def test_bind_preserves_declared_parameter_types(started_cluster):
    # Preserve declared OID types while keeping values inside quoted cast arguments.
    node = started_cluster.instances["node"]

    ch = psycopg.connect(
        host=node.ip_address,
        port=server_port,
        user="default",
        password="123",
    )
    pg = ch.pgconn

    # int4 OID = 23; it must support arithmetic.
    res_add = pg.exec_params(b"SELECT $1 + 1", [b"41"], [23], [0], 0)
    assert res_add.status == psycopg.pq.ExecStatus.TUPLES_OK, res_add.error_message
    assert res_add.get_value(0, 0) == b"42"

    # `LIMIT` requires the bound value to retain its numeric type.
    setup = ch.cursor()
    setup.execute("DROP TABLE IF EXISTS bind_num_t;")
    setup.execute("CREATE TABLE bind_num_t (x Int32) ENGINE = Memory;")
    setup.execute("INSERT INTO bind_num_t VALUES (1), (2), (3), (4), (5);")

    res_limit = pg.exec_params(
        b"SELECT count() FROM (SELECT x FROM bind_num_t ORDER BY x LIMIT $1)",
        [b"2"],
        [23],
        [0],
        0,
    )
    assert res_limit.status == psycopg.pq.ExecStatus.TUPLES_OK, (
        res_limit.error_message
    )
    assert res_limit.get_value(0, 0) == b"2"

    # An invalid int4 payload remains inside the cast argument.
    res_inj = pg.exec_params(
        b"SELECT $1", [b"1 UNION ALL SELECT 42"], [23], [0], 0
    )
    assert res_inj.status == psycopg.pq.ExecStatus.FATAL_ERROR

    # Numeric-looking SQL fragments are invalid values, not expressions.
    for payload in (b"1--", b"1+2", b"1-2"):
        res_bad = pg.exec_params(
            b"SELECT count() FROM bind_num_t WHERE x = $1 AND x = 42",
            [payload],
            [23],
            [0],
            0,
        )
        assert res_bad.status == psycopg.pq.ExecStatus.FATAL_ERROR, payload

    # Validate against the declared type, not the value's lexical shape.
    res_int_frac = pg.exec_params(b"SELECT $1 + 1", [b"3.14"], [23], [0], 0)
    assert res_int_frac.status == psycopg.pq.ExecStatus.FATAL_ERROR

    # Enforce the declared integer width.
    for oid, payload in (
        (23, b"2147483648"),           # int4 max + 1
        (21, b"32768"),                # int2 max + 1
        (20, b"9223372036854775808"),  # int8 max + 1
        (26, b"4294967296"),           # oid (UInt32) max + 1
    ):
        res_range = pg.exec_params(b"SELECT $1", [payload], [oid], [0], 0)
        assert res_range.status == psycopg.pq.ExecStatus.FATAL_ERROR, (oid, payload)

    # An in-range value still works and keeps its declared type.
    res_ok = pg.exec_params(b"SELECT $1", [b"32000"], [21], [0], 0)
    assert res_ok.status == psycopg.pq.ExecStatus.TUPLES_OK, res_ok.error_message
    assert res_ok.get_value(0, 0) == b"32000"

    # `oid` (OID 26) is unsigned.
    res_oid_neg = pg.exec_params(b"SELECT $1", [b"-1"], [26], [0], 0)
    assert res_oid_neg.status == psycopg.pq.ExecStatus.FATAL_ERROR

    # Preserve non-numeric declared OIDs, starting with bool (OID 16).
    res_bool = pg.exec_params(b"SELECT NOT $1", [b"true"], [16], [0], 0)
    assert res_bool.status == psycopg.pq.ExecStatus.TUPLES_OK, res_bool.error_message
    # Accept equivalent boolean text renderings across versions.
    assert res_bool.get_value(0, 0) in (b"f", b"false", b"0")
    res_bool_type = pg.exec_params(b"SELECT toTypeName($1)", [b"true"], [16], [0], 0)
    assert res_bool_type.get_value(0, 0) == b"Bool", res_bool_type.get_value(0, 0)

    # date (OID 1082) preserves type and rejects invalid values.
    res_date = pg.exec_params(b"SELECT toTypeName($1)", [b"2024-01-15"], [1082], [0], 0)
    assert res_date.status == psycopg.pq.ExecStatus.TUPLES_OK, res_date.error_message
    assert res_date.get_value(0, 0).startswith(b"Date"), res_date.get_value(0, 0)
    res_date_bad = pg.exec_params(b"SELECT $1", [b"not-a-date"], [1082], [0], 0)
    assert res_date_bad.status == psycopg.pq.ExecStatus.FATAL_ERROR

    # uuid (OID 2950) preserves type and contains invalid payloads.
    res_uuid = pg.exec_params(
        b"SELECT toTypeName($1)",
        [b"61f0c404-5cb3-11e7-907b-a6006ad3dba0"],
        [2950],
        [0],
        0,
    )
    assert res_uuid.status == psycopg.pq.ExecStatus.TUPLES_OK, res_uuid.error_message
    assert res_uuid.get_value(0, 0) == b"UUID", res_uuid.get_value(0, 0)
    res_uuid_inj = pg.exec_params(b"SELECT $1", [b"x' OR 1=1--"], [2950], [0], 0)
    assert res_uuid_inj.status == psycopg.pq.ExecStatus.FATAL_ERROR

    # `numeric` (OID 1700) round-trips through an exact `Decimal`.
    res_num_type = pg.exec_params(b"SELECT toTypeName($1)", [b"2.11"], [1700], [0], 0)
    assert res_num_type.status == psycopg.pq.ExecStatus.TUPLES_OK, (
        res_num_type.error_message
    )
    assert res_num_type.get_value(0, 0).startswith(b"Decimal"), (
        res_num_type.get_value(0, 0)
    )
    res_num_val = pg.exec_params(b"SELECT $1", [b"2.11"], [1700], [0], 0)
    assert res_num_val.status == psycopg.pq.ExecStatus.TUPLES_OK, (
        res_num_val.error_message
    )
    assert res_num_val.get_value(0, 0) == b"2.11", res_num_val.get_value(0, 0)

    # `numeric` accepts only one numeric literal.
    for payload in (b"1--", b"1+2"):
        res_num_bad = pg.exec_params(b"SELECT $1", [payload], [1700], [0], 0)
        assert res_num_bad.status == psycopg.pq.ExecStatus.FATAL_ERROR, payload
        assert b"prepared-statement parameter" in res_num_bad.error_message, payload

    # Huge exponents are rejected before arithmetic or zero-padding.
    for payload in (b"1e1000000", b"1e-1000000", b"1e99999999999999999999"):
        res_exp = pg.exec_params(b"SELECT $1", [payload], [1700], [0], 0)
        assert res_exp.status == psycopg.pq.ExecStatus.FATAL_ERROR, payload

    # timestamptz (OID 1184) preserves UTC semantics.
    res_tstz = pg.exec_params(
        b"SELECT toTypeName($1)", [b"2024-01-15 12:30:45+02"], [1184], [0], 0
    )
    assert res_tstz.status == psycopg.pq.ExecStatus.TUPLES_OK, res_tstz.error_message
    assert b"UTC" in res_tstz.get_value(0, 0), res_tstz.get_value(0, 0)

    setup.execute("DROP TABLE bind_num_t;")
    ch.close()


def test_bind_unspecified_oid_infers_type(started_cluster):
    # OID 0 requests inference: preserve safe literals and quote everything else.
    node = started_cluster.instances["node"]

    ch = psycopg.connect(
        host=node.ip_address,
        port=server_port,
        user="default",
        password="123",
    )
    pg = ch.pgconn

    # An inferred integer supports arithmetic.
    res_add = pg.exec_params(b"SELECT $1 + 1", [b"41"], [0], [0], 0)
    assert res_add.status == psycopg.pq.ExecStatus.TUPLES_OK, res_add.error_message
    assert res_add.get_value(0, 0) == b"42"

    # An inferred integer works in `LIMIT`.
    setup = ch.cursor()
    setup.execute("DROP TABLE IF EXISTS bind_infer_t;")
    setup.execute("CREATE TABLE bind_infer_t (x Int32) ENGINE = Memory;")
    setup.execute("INSERT INTO bind_infer_t VALUES (1), (2), (3), (4), (5);")

    res_limit = pg.exec_params(
        b"SELECT count() FROM (SELECT x FROM bind_infer_t ORDER BY x LIMIT $1)",
        [b"2"],
        [0],
        [0],
        0,
    )
    assert res_limit.status == psycopg.pq.ExecStatus.TUPLES_OK, res_limit.error_message
    assert res_limit.get_value(0, 0) == b"2"

    # A null `paramTypes` array also requests inference.
    res_none = pg.exec_params(b"SELECT $1 + 1", [b"41"])
    assert res_none.status == psycopg.pq.ExecStatus.TUPLES_OK, res_none.error_message
    assert res_none.get_value(0, 0) == b"42"

    # Exact boolean keywords infer as `Bool`.
    res_bool = pg.exec_params(b"SELECT NOT $1", [b"true"], [0], [0], 0)
    assert res_bool.status == psycopg.pq.ExecStatus.TUPLES_OK, res_bool.error_message
    # `NOT true` is false, rendered as `f`/`false`/`0` depending on the text format.
    assert res_bool.get_value(0, 0) in (b"f", b"false", b"0"), res_bool.get_value(0, 0)

    # Other inferred values remain quoted text.
    res_text = pg.exec_params(b"SELECT $1", [b"hello"], [0], [0], 0)
    assert res_text.status == psycopg.pq.ExecStatus.TUPLES_OK, res_text.error_message
    assert res_text.get_value(0, 0) == b"hello"

    # A boolean with trailing syntax stays quoted.
    res_bool_inj = pg.exec_params(b"SELECT $1", [b"true; DROP TABLE bind_infer_t"], [0], [0], 0)
    assert res_bool_inj.status == psycopg.pq.ExecStatus.TUPLES_OK, res_bool_inj.error_message
    assert res_bool_inj.get_value(0, 0) == b"true; DROP TABLE bind_infer_t"

    # Numeric-looking SQL fragments stay quoted.
    res_inj = pg.exec_params(
        b"SELECT count() FROM bind_infer_t WHERE x = $1 AND x = 42",
        [b"1--"],
        [0],
        [0],
        0,
    )
    # A type error is safe; the trailing predicate must remain active.
    if res_inj.status == psycopg.pq.ExecStatus.TUPLES_OK:
        assert res_inj.get_value(0, 0) == b"0", res_inj.get_value(0, 0)

    setup.execute("DROP TABLE bind_infer_t;")
    ch.close()


def test_bind_error_keeps_connection_alive(started_cluster):
    # Extended-query errors recover at `Sync` without closing the connection.
    node = started_cluster.instances["node"]

    ch = psycopg.connect(
        host=node.ip_address,
        port=server_port,
        user="default",
        password="123",
    )
    pg = ch.pgconn

    # A rejected int4 bind returns an error, not a closed connection.
    res_bad = pg.exec_params(b"SELECT $1", [b"1--"], [23], [0], 0)
    assert res_bad.status == psycopg.pq.ExecStatus.FATAL_ERROR

    # The SAME connection is still usable for a valid query afterwards.
    res_ok = pg.exec_params(b"SELECT $1 + 1", [b"41"], [23], [0], 0)
    assert res_ok.status == psycopg.pq.ExecStatus.TUPLES_OK, res_ok.error_message
    assert res_ok.get_value(0, 0) == b"42"

    # Several consecutive errors on one connection all recover.
    for payload in (b"1+2", b"2147483648", b"3.14"):
        res_e = pg.exec_params(b"SELECT $1", [payload], [23], [0], 0)
        assert res_e.status == psycopg.pq.ExecStatus.FATAL_ERROR, payload
        res_after = pg.exec_params(b"SELECT $1 + 1", [b"1"], [23], [0], 0)
        assert res_after.status == psycopg.pq.ExecStatus.TUPLES_OK, (
            payload,
            res_after.error_message,
        )
        assert res_after.get_value(0, 0) == b"2", payload

    ch.close()


def _pg_raw_extended_query_session(node, backend_key=None, port=None):
    # Minimal raw client for protocol messages hidden by libpq and psycopg.
    # `backend_key`, when given, receives the `BackendKeyData` pair as {"pid": .., "key": ..};
    # libpq keeps the cancellation secret private, so only a raw client can read it.
    sock = socket.create_connection((node.ip_address, port or server_port), timeout=10)

    def read_until_ready(timeout=10.0):
        # Read backend message types through `ReadyForQuery` (`Z`).
        sock.settimeout(timeout)
        buf = b""
        types = []
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            chunk = sock.recv(65536)
            if not chunk:
                break
            buf += chunk
            while len(buf) >= 5:
                mtype = chr(buf[0])
                (mlen,) = struct.unpack("!I", buf[1:5])
                if len(buf) < 1 + mlen:
                    break
                if mtype == "K" and backend_key is not None:
                    pid, key = struct.unpack("!iI", buf[5 : 1 + mlen])
                    backend_key.update({"pid": pid, "key": key})
                types.append(mtype)
                buf = buf[1 + mlen :]
            if types and types[-1] == "Z":
                return types
        return types

    # Startup + cleartext-password auth (the 'default' user has password '123').
    params = b"user\x00default\x00database\x00default\x00\x00"
    sock.sendall(struct.pack("!I", 8 + len(params)) + struct.pack("!I", 196608) + params)
    sock.settimeout(10)
    auth = sock.recv(65536)
    assert auth[0:1] == b"R", "expected authentication request"
    (auth_code,) = struct.unpack("!I", auth[5:9])
    if auth_code == 3:  # AuthenticationCleartextPassword
        pwd = b"123\x00"
        sock.sendall(b"p" + struct.pack("!I", 4 + len(pwd)) + pwd)
    read_until_ready()  # drain to the first ReadyForQuery
    return sock, read_until_ready


def _fe(t, body):
    return t.encode() + struct.pack("!I", 4 + len(body)) + body


def test_extended_query_ready_for_query_and_describe(started_cluster):
    # Each `Sync` produces one `ReadyForQuery`; `Describe` defers row layout to `Execute`.
    node = started_cluster.instances["node"]

    def sync():
        return _fe("S", b"")

    def describe(kind, name):
        return _fe("D", kind.encode() + name.encode() + b"\x00")

    def close(kind, name):
        return _fe("C", kind.encode() + name.encode() + b"\x00")

    def parse(stmt, query, oids):
        b = stmt.encode() + b"\x00" + query.encode() + b"\x00" + struct.pack("!H", len(oids))
        for o in oids:
            b += struct.pack("!I", o)
        return _fe("P", b)

    def bind(portal, stmt, values):
        b = portal.encode() + b"\x00" + stmt.encode() + b"\x00" + struct.pack("!H", 0)
        b += struct.pack("!H", len(values))
        for v in values:
            vb = v.encode()
            b += struct.pack("!i", len(vb)) + vb
        b += struct.pack("!H", 0)
        return _fe("B", b)

    def execute(portal):
        return _fe("E", portal.encode() + b"\x00" + struct.pack("!I", 0))

    # Closing an unknown statement is a successful no-op.
    sock, read_until_ready = _pg_raw_extended_query_session(node)
    sock.sendall(close("S", "nope") + sync())
    types = read_until_ready()
    assert types.count("Z") == 1, f"Close/Sync must emit one ReadyForQuery, got {types}"
    assert "3" in types, f"Close must respond with CloseComplete, got {types}"
    sock.close()

    # `Describe` emits no mid-cycle `ReadyForQuery`.
    sock, read_until_ready = _pg_raw_extended_query_session(node)
    sock.sendall(describe("S", "") + sync())
    types = read_until_ready()
    assert types.count("Z") == 1, f"Describe/Sync must emit one ReadyForQuery, got {types}"
    sock.close()

    # `Describe` does not abort a complete extended-query cycle.
    sock, read_until_ready = _pg_raw_extended_query_session(node)
    sock.sendall(
        parse("", "SELECT $1", (23,))
        + bind("", "", ("5",))
        + describe("P", "")
        + execute("")
        + sync()
    )
    types = read_until_ready()
    assert types.count("Z") == 1, f"P/B/D/E/S must emit one ReadyForQuery, got {types}"
    assert "E" not in types, f"P/B/D/E/S must not error on the Describe no-op, got {types}"
    assert "T" in types and "C" in types, f"Execute must run and return rows, got {types}"
    sock.close()

    # A rejected typed `Bind` still recovers at `Sync`.
    sock, read_until_ready = _pg_raw_extended_query_session(node)
    sock.sendall(parse("", "SELECT $1 AS a", (23,)) + bind("", "", ("1--",)) + execute("") + sync())
    types = read_until_ready()
    assert "E" in types, f"1-- injection must be rejected, got {types}"
    assert types.count("Z") == 1, f"rejected Bind must emit one ReadyForQuery, got {types}"
    sock.sendall(_fe("Q", b"SELECT 7\x00"))
    types = read_until_ready()
    assert "C" in types, f"connection must stay alive after a rejected Bind, got {types}"
    sock.close()


def test_malformed_extended_message_recovers(started_cluster):
    # Reject malformed extended messages without desynchronizing recovery.
    node = started_cluster.instances["node"]

    def sync():
        return _fe("S", b"")

    # Put a complete-looking `Sync` frame after each invalid count, but include it in the
    # malformed message's declared payload. Recovery must ignore it and wait for the real `Sync`.
    def parse_neg_num_params():
        b = b"\x00" + b"SELECT 1\x00" + struct.pack("!h", -1) + sync()
        return _fe("P", b)

    # Negative parameter-format-code count.
    def bind_neg_param_formats():
        b = b"\x00" + b"\x00" + struct.pack("!h", -1) + sync()
        return _fe("B", b)

    # Negative parameter count.
    def bind_neg_num_params():
        b = b"\x00" + b"\x00" + struct.pack("!H", 0) + struct.pack("!h", -1) + sync()
        return _fe("B", b)

    # Negative result-format-code count.
    def bind_neg_result_formats():
        b = (
            b"\x00"
            + b"\x00"
            + struct.pack("!H", 0)
            + struct.pack("!H", 0)
            + struct.pack("!h", -1)
            + sync()
        )
        return _fe("B", b)

    # `Describe` must not read the following `Sync` as its missing payload.
    def describe_incomplete_payload():
        return _fe("D", b"")

    # A named portal is rejected after deserialization. The embedded `Sync` must
    # remain in the `Execute` payload until recovery reaches the real `Sync`.
    def execute_named_portal():
        b = b"named\x00" + struct.pack("!I", 0) + sync()
        return _fe("E", b)

    # An invalid close target is rejected after deserialization for the same reason.
    def close_invalid_target():
        return _fe("C", b"X\x00" + sync())

    for make_message in (
        parse_neg_num_params,
        bind_neg_param_formats,
        bind_neg_num_params,
        bind_neg_result_formats,
        describe_incomplete_payload,
        execute_named_portal,
        close_invalid_target,
    ):
        sock, read_until_ready = _pg_raw_extended_query_session(node)
        sock.sendall(make_message() + sync())
        types = read_until_ready()
        assert "E" in types, f"malformed message must be rejected, got {types}"
        assert types.count("Z") == 1, (
            f"malformed message must emit one ReadyForQuery per real Sync, got {types}"
        )
        # The same connection must stay usable (stream stayed aligned).
        sock.sendall(_fe("Q", b"SELECT 7\x00"))
        types = read_until_ready()
        assert "C" in types, f"connection must stay alive after malformed message, got {types}"
        sock.close()


def test_incomplete_simple_query_payload_recovers(started_cluster):
    # A length-only `Query` must not wait for or consume the next frontend message.
    node = started_cluster.instances["node"]
    sock, read_until_ready = _pg_raw_extended_query_session(node)
    sock.sendall(_fe("Q", b""))
    types = read_until_ready()
    assert "E" in types, f"incomplete Query must be rejected, got {types}"
    assert types.count("Z") == 1, (
        f"incomplete Query must emit one ReadyForQuery, got {types}"
    )

    sock.sendall(_fe("Q", b"SELECT 7\x00"))
    types = read_until_ready()
    assert "C" in types, f"connection must stay usable after incomplete Query, got {types}"
    sock.close()


def test_flush_error_discards_until_sync(started_cluster):
    # A `FLUSH` error discards the rest of the cycle through `Sync`.
    node = started_cluster.instances["node"]

    def sync():
        return _fe("S", b"")

    def flush():
        return _fe("H", b"")

    def parse(stmt, query, oids):
        b = stmt.encode() + b"\x00" + query.encode() + b"\x00" + struct.pack("!H", len(oids))
        for o in oids:
            b += struct.pack("!I", o)
        return _fe("P", b)

    def bind(portal, stmt, values):
        b = portal.encode() + b"\x00" + stmt.encode() + b"\x00" + struct.pack("!H", 0)
        b += struct.pack("!H", len(values))
        for v in values:
            vb = v.encode()
            b += struct.pack("!i", len(vb)) + vb
        b += struct.pack("!H", 0)
        return _fe("B", b)

    def execute(portal):
        return _fe("E", portal.encode() + b"\x00" + struct.pack("!I", 0))

    sock, read_until_ready = _pg_raw_extended_query_session(node)
    sock.sendall(
        parse("", "SELECT $1 AS a", (23,))
        + bind("", "", ("5",))
        + flush()
        + execute("")
        + sync()
    )
    types = read_until_ready()
    assert "E" in types, f"FLUSH must produce an ErrorResponse, got {types}"
    # The Execute after the FLUSH error must be discarded, not run.
    assert "T" not in types and "D" not in types and "C" not in types, (
        f"Execute after FLUSH error must be discarded until Sync, got {types}"
    )
    assert types.count("Z") == 1, (
        f"FLUSH pipeline must emit one ReadyForQuery per Sync, got {types}"
    )
    # The same connection must stay usable.
    sock.sendall(_fe("Q", b"SELECT 7\x00"))
    types = read_until_ready()
    assert "C" in types, f"connection must stay alive after FLUSH error, got {types}"
    sock.close()


def test_bind_binary_result_format_accepted_as_text(started_cluster):
    # Accept binary result requests while returning correctly advertised text.
    node = started_cluster.instances["node"]

    def sync():
        return _fe("S", b"")

    def parse(stmt, query, oids):
        b = stmt.encode() + b"\x00" + query.encode() + b"\x00" + struct.pack("!H", len(oids))
        for o in oids:
            b += struct.pack("!I", o)
        return _fe("P", b)

    # Bind with one text parameter and a single binary (1) result format code.
    def bind_binary_result(portal, stmt, values):
        b = portal.encode() + b"\x00" + stmt.encode() + b"\x00" + struct.pack("!H", 0)
        b += struct.pack("!H", len(values))
        for v in values:
            vb = v.encode()
            b += struct.pack("!i", len(vb)) + vb
        b += struct.pack("!H", 1) + struct.pack("!H", 1)  # one result format code = binary
        return _fe("B", b)

    def execute(portal):
        return _fe("E", portal.encode() + b"\x00" + struct.pack("!I", 0))

    sock, read_until_ready = _pg_raw_extended_query_session(node)
    sock.sendall(
        parse("", "SELECT $1 AS a", (23,))
        + bind_binary_result("", "", ("5",))
        + execute("")
        + sync()
    )
    types = read_until_ready()
    assert "E" not in types, f"binary result format must be accepted, not rejected, got {types}"
    assert "C" in types, f"query must complete normally, got {types}"
    assert types.count("Z") == 1, (
        f"a single Sync must emit exactly one ReadyForQuery, got {types}"
    )
    # The same connection must stay usable (stream stayed aligned).
    sock.sendall(_fe("Q", b"SELECT 7\x00"))
    types = read_until_ready()
    assert "C" in types, f"connection must stay alive after query, got {types}"
    sock.close()


def test_standalone_sync_emits_one_ready_for_query(started_cluster):
    # A standalone `Sync` produces exactly one `ReadyForQuery`.
    node = started_cluster.instances["node"]

    def sync():
        return _fe("S", b"")

    sock, read_until_ready = _pg_raw_extended_query_session(node)
    # A single standalone Sync: exactly one ReadyForQuery.
    sock.sendall(sync())
    types = read_until_ready()
    assert types.count("Z") == 1, f"standalone Sync must emit one ReadyForQuery, got {types}"

    # Read consecutive `Sync` replies separately to count them exactly.
    for _ in range(3):
        sock.sendall(sync())
        types = read_until_ready()
        assert types.count("Z") == 1, (
            f"each standalone Sync must emit one ReadyForQuery, got {types}"
        )

    # The connection stays usable for a normal query afterwards.
    sock.sendall(_fe("Q", b"SELECT 7\x00"))
    types = read_until_ready()
    assert "C" in types, f"connection must stay alive after standalone Syncs, got {types}"
    sock.close()


def test_bind_requires_exact_placeholder_count(started_cluster):
    # `Bind` arity follows the highest `$N`, not the number of declared OIDs.
    node = started_cluster.instances["node"]

    def sync():
        return _fe("S", b"")

    # Parse with NO declared OIDs but two placeholders.
    def parse(stmt, query, oids):
        b = stmt.encode() + b"\x00" + query.encode() + b"\x00" + struct.pack("!H", len(oids))
        for o in oids:
            b += struct.pack("!I", o)
        return _fe("P", b)

    def bind(portal, stmt, values):
        b = portal.encode() + b"\x00" + stmt.encode() + b"\x00" + struct.pack("!H", 0)
        b += struct.pack("!H", len(values))
        for v in values:
            vb = v.encode()
            b += struct.pack("!i", len(vb)) + vb
        b += struct.pack("!H", 0)
        return _fe("B", b)

    def execute(portal):
        return _fe("E", portal.encode() + b"\x00" + struct.pack("!I", 0))

    # Reject one value for two placeholders.
    sock, read_until_ready = _pg_raw_extended_query_session(node)
    sock.sendall(
        parse("", "SELECT $1, $2", ())
        + bind("", "", ("1",))
        + execute("")
        + sync()
    )
    types = read_until_ready()
    assert "E" in types, f"too-few values for two placeholders must be rejected, got {types}"
    assert "C" not in types, f"Execute must not run after the arity error, got {types}"
    assert types.count("Z") == 1, f"arity error must emit one ReadyForQuery, got {types}"

    # Reject two values for one placeholder.
    sock.sendall(
        parse("", "SELECT $1", ())
        + bind("", "", ("1", "2"))
        + execute("")
        + sync()
    )
    types = read_until_ready()
    assert "E" in types, f"too-many values for one placeholder must be rejected, got {types}"
    assert types.count("Z") == 1, f"arity error must emit one ReadyForQuery, got {types}"

    # Exactly matching arity still works end to end.
    sock.sendall(
        parse("", "SELECT $1, $2", ())
        + bind("", "", ("10", "20"))
        + execute("")
        + sync()
    )
    types = read_until_ready()
    assert "E" not in types, f"matching arity must not error, got {types}"
    assert "C" in types, f"matching arity must execute, got {types}"
    assert types.count("Z") == 1, f"matching arity must emit one ReadyForQuery, got {types}"
    sock.close()


def test_execute_no_sql_injection(started_cluster):
    # Simple `EXECUTE` arguments must also remain safe SQL literals.
    node = started_cluster.instances["node"]

    def connect():
        return psycopg.connect(
            host=node.ip_address,
            port=server_port,
            user="default",
            password="123",
        )

    ch = connect()
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

    # Exercise a bare placeholder; quoted `$1` is not a placeholder token.
    inj = connect()
    inj_cur = inj.cursor()
    inj_cur.execute("PREPARE by_id_inj AS SELECT name FROM exec_users WHERE id = $1;")
    leaked = []
    try:
        inj_cur.execute(
            "EXECUTE by_id_inj('1 UNION ALL SELECT secret FROM exec_secret -- ');"
        )
        leaked = inj_cur.fetchall()
    except psycopg.Error:
        pass
    assert ("TOP_SECRET",) not in leaked
    inj.close()

    # The same payload round-trips as one string value.
    cur.execute("PREPARE echo_inj AS SELECT $1;")
    cur.execute("EXECUTE echo_inj('1 UNION ALL SELECT secret FROM exec_secret -- ');")
    assert cur.fetchall() == [("1 UNION ALL SELECT secret FROM exec_secret -- ",)]

    # An argument containing a single quote round-trips as data.
    cur.execute("PREPARE echo_one AS SELECT $1 AS v;")
    cur.execute("EXECUTE echo_one('O''Brien');")
    assert cur.fetchall() == [("O'Brien",)]

    cur.execute("DEALLOCATE by_id;")
    cur.execute("DEALLOCATE by_name;")
    cur.execute("DEALLOCATE echo_inj;")
    cur.execute("DEALLOCATE echo_one;")
    cur.execute("DROP TABLE exec_users;")
    cur.execute("DROP TABLE exec_secret;")


def test_execute_requires_exact_argument_count(started_cluster):
    # Simple `PREPARE`/`EXECUTE` requires exact arity too.
    node = started_cluster.instances["node"]

    def connect():
        return psycopg.connect(
            host=node.ip_address,
            port=server_port,
            user="default",
            password="123",
        )

    # Use a fresh connection after each rejected `EXECUTE`.
    ch = connect()
    cur = ch.cursor()
    cur.execute("PREPARE one_arg AS SELECT $1 AS v;")
    cur.execute("EXECUTE one_arg(7);")
    assert cur.fetchall() == [(7,)]
    ch.close()

    # Over-supply: an extra argument must be rejected, not silently dropped.
    ch = connect()
    cur = ch.cursor()
    cur.execute("PREPARE over_arg AS SELECT $1 AS v;")
    rejected = False
    try:
        cur.execute("EXECUTE over_arg(1, 2);")
        cur.fetchall()
    except psycopg.Error:
        rejected = True
    assert rejected, "EXECUTE with too many arguments must be rejected"
    ch.close()

    # Under-supply: a missing argument must be rejected, not leave `$2` in the SQL.
    ch = connect()
    cur = ch.cursor()
    cur.execute("PREPARE under_arg AS SELECT $1, $2;")
    rejected = False
    try:
        cur.execute("EXECUTE under_arg(1);")
        cur.fetchall()
    except psycopg.Error:
        rejected = True
    assert rejected, "EXECUTE with too few arguments must be rejected"
    ch.close()

    # A zero-placeholder statement rejects any argument.
    ch = connect()
    cur = ch.cursor()
    cur.execute("PREPARE no_arg AS SELECT 1;")
    rejected = False
    try:
        cur.execute("EXECUTE no_arg(1);")
        cur.fetchall()
    except psycopg.Error:
        rejected = True
    assert rejected, "EXECUTE with an argument for a zero-parameter statement must be rejected"
    ch.close()


def test_execute_rejects_non_literal_arguments(started_cluster):
    # Reject expressions whose substitution would change precedence or evaluation count.
    node = started_cluster.instances["node"]

    def connect():
        return psycopg.connect(
            host=node.ip_address,
            port=server_port,
            user="default",
            password="123",
        )

    def rejected(prepare_sql, execute_sql):
        # A rejected `EXECUTE` requires a fresh connection.
        ch = connect()
        cur = ch.cursor()
        try:
            cur.execute(prepare_sql)
            try:
                cur.execute(execute_sql)
                cur.fetchall()
                return False
            except psycopg.errors.Error:
                return True
        finally:
            ch.close()

    # Reject arithmetic, function calls, and injection-shaped expressions.
    assert rejected("PREPARE expr_arith AS SELECT $1 AS v;", "EXECUTE expr_arith(1 + 1);")
    assert rejected("PREPARE expr_func AS SELECT $1 AS v;", "EXECUTE expr_func(abs(-5));")
    assert rejected("PREPARE expr_rand AS SELECT $1 AS v;", "EXECUTE expr_rand(rand());")
    assert rejected(
        "PREPARE expr_concat AS SELECT $1 AS v;",
        "EXECUTE expr_concat(concat('1 UNION ALL SELECT 2', ' -- '));",
    )

    # Reject a precedence-sensitive expression.
    assert rejected("PREPARE expr_prec AS SELECT $1 * 10 AS v;", "EXECUTE expr_prec(1 + 1);")

    # Negative numbers are literals; normalize their protocol text for comparison.
    ch = connect()
    cur = ch.cursor()
    cur.execute("PREPARE expr_neg AS SELECT $1 AS v;")
    cur.execute("EXECUTE expr_neg(-7);")
    assert [(int(v),) for (v,) in cur.fetchall()] == [(-7,)]
    ch.close()


def test_execute_zero_parameter_statement(started_cluster):
    # Accept both PostgreSQL forms of zero-parameter `EXECUTE`.
    node = started_cluster.instances["node"]

    ch = psycopg.connect(
        host=node.ip_address,
        port=server_port,
        user="default",
        password="123",
    )
    cur = ch.cursor()

    cur.execute("PREPARE zero_arity AS SELECT 42 AS v;")
    # Parentheses omitted.
    cur.execute("EXECUTE zero_arity;")
    assert cur.fetchall() == [(42,)]
    # Empty parentheses.
    cur.execute("EXECUTE zero_arity();")
    assert cur.fetchall() == [(42,)]

    ch.close()


def test_execute_negative_argument_stays_a_separate_token(started_cluster):
    # A substituted argument must not merge with the token before it: `5-$1` bound to `-1` would
    # otherwise read as `5--1`, whose `--` comments out the rest of the statement.
    node = started_cluster.instances["node"]

    ch = psycopg.connect(
        host=node.ip_address,
        port=server_port,
        user="default",
        password="123",
    )
    cur = ch.cursor()

    cur.execute("PREPARE negative_argument AS SELECT 5-$1 AS v, 'tail' AS t;")
    cur.execute("EXECUTE negative_argument(-1);")
    assert cur.fetchall() == [(6, "tail")]
    # Control: the same statement with an argument that cannot merge.
    cur.execute("EXECUTE negative_argument(1);")
    assert cur.fetchall() == [(4, "tail")]

    ch.close()


def test_copy_no_sql_injection(started_cluster):
    # Treat client-supplied `COPY` table and column names as identifiers.
    node = started_cluster.instances["node"]

    def connect():
        # `with connection` manages transactions but does not close the connection.
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

    # Use a fresh connection after each rejected `COPY`.

    # A quoted injection remains one table name.
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

    # A quoted injection remains one `COPY FROM` column name.
    with connect() as c, pytest.raises(Exception):
        c.cursor().copy_expert(
            'COPY copy_load ("s) SELECT s FROM copy_secret_str -- ") FROM STDIN',
            StringIO("x\n"),
        )
    # The injected SELECT must not have run: the load table stays empty.
    # ClickHouse returns the count over the PostgreSQL wire as text, so cast it.
    setup_cur.execute("SELECT count() FROM copy_load WHERE s = 'TOP_SECRET';")
    assert int(setup_cur.fetchone()[0]) == 0

    # A quoted injection remains one `COPY FROM` table name. The rendered identifier is backquoted,
    # so the payload carries a backquote of its own to attack the escaping rather than the quoting.
    with connect() as c, pytest.raises(Exception):
        c.cursor().copy_expert(
            "COPY \"copy_load` (s) SELECT s FROM copy_secret_str -- \" FROM STDIN",
            StringIO("x\n"),
        )
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


def _assert_cancel_request_does_not_cancel_http_query(node, query_id, pid, key):
    """Runs a long HTTP query under `query_id`, sends an unauthenticated PostgreSQL CancelRequest
    naming (`pid`, `key`) while the query is running, and asserts the query is not affected."""
    result = {}

    def run_http_query():
        try:
            # Consume `sleepEachRow` so optimization cannot remove the delay.
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
            # Poll over HTTP to avoid slow `docker exec` round trips under sanitizers.
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


def _pg_query_id_from_log(node, marker):
    """The query id a PostgreSQL statement ran under. A client is never told it, and it carries a
    random token, so a test that needs it has to read it back from the server."""
    node.query("SYSTEM FLUSH LOGS query_log", password="123")
    return node.query(
        "SELECT query_id FROM system.query_log"
        f" WHERE query_id LIKE 'postgres:%' AND query LIKE '%{marker}%' AND type = 'QueryFinish'"
        " ORDER BY event_time_microseconds DESC LIMIT 1",
        password="123",
    ).strip()


def test_cancel_request_does_not_cancel_foreign_query(started_cluster):
    """An unauthenticated PostgreSQL CancelRequest may only cancel queries that actually run on the
    PostgreSQL interface. Any other interface lets a client choose its own query id, so a query that
    takes the query id of a live PostgreSQL connection must not be cancellable this way, not even by
    that connection's genuine `BackendKeyData` pair."""
    node = started_cluster.instances["node"]

    backend_key = {}
    sock, read_until_ready = _pg_raw_extended_query_session(node, backend_key)
    with sock:
        # One statement, so the connection's query id can be read back, then the connection idles.
        sock.sendall(_fe("Q", b"SELECT 20250808\x00"))
        assert "Z" in read_until_ready()
        assert backend_key, "the server did not send BackendKeyData"
        query_id = _pg_query_id_from_log(node, "20250808")
        assert query_id.startswith(f"postgres:{backend_key['pid']}:"), query_id

        _assert_cancel_request_does_not_cancel_http_query(
            node, query_id, backend_key["pid"], backend_key["key"]
        )


def test_cancel_request_does_not_cancel_query_reusing_freed_id(started_cluster):
    """Once a PostgreSQL connection is gone, its query id is free for any client to pick on another
    interface. A CancelRequest carrying that connection's once-valid pair must not cancel the later
    query: the cancel is bound to a query that was verified to run on the PostgreSQL interface, not
    to whatever currently holds the id. Two guards reach that outcome here, the interface check and
    the pair leaving the registry at teardown, and this case does not tell them apart: isolating the
    second would need a PostgreSQL connection id to be handed out twice."""
    node = started_cluster.instances["node"]

    # Obtain a server-assigned PostgreSQL query ID together with its credential, then free both.
    backend_key = {}
    sock, read_until_ready = _pg_raw_extended_query_session(node, backend_key)
    with sock:
        sock.sendall(_fe("Q", b"SELECT 20250807\x00"))
        assert "Z" in read_until_ready()
    assert backend_key, "the server did not send BackendKeyData"

    query_id = _pg_query_id_from_log(node, "20250807")
    # The credential is not part of the query id, so `system.query_log` does not publish it.
    assert query_id.startswith(f"postgres:{backend_key['pid']}:"), query_id
    assert str(backend_key["key"]) not in query_id, (query_id, backend_key)

    _assert_cancel_request_does_not_cancel_http_query(
        node, query_id, backend_key["pid"], backend_key["key"]
    )


def test_learned_query_id_does_not_block_the_next_statement(started_cluster):
    """A query id may be held by one query at a time server-wide, and PostgreSQL query ids are
    published by `system.processes` and `system.query_log` while the cancellation secret is not. So
    the id is the part an observer can learn, and an id that outlived its statement would let
    whoever read it keep that connection's next statement out of the process list from any other
    interface, without holding `KILL QUERY`."""
    node = started_cluster.instances["node"]

    backend_key = {}
    sock, read_until_ready = _pg_raw_extended_query_session(node, backend_key)
    with sock:
        sock.sendall(_fe("Q", b"SELECT 'pg_first_statement_20250903'\x00"))
        assert "Z" in read_until_ready()
        assert backend_key, "the server did not send BackendKeyData"
        first_id = _pg_query_id_from_log(node, "pg_first_statement_20250903")
        assert first_id.startswith(f"postgres:{backend_key['pid']}:"), first_id

        # Hold the learned id on another interface for longer than the next statement takes.
        held = {}

        def hold_the_learned_id():
            try:
                # Consume `sleepEachRow` so optimization cannot remove the delay.
                held["output"] = node.http_query(
                    "SELECT sum(sleepEachRow(0.3) + number) FROM numbers(60)",
                    params={"query_id": first_id, "max_block_size": "1"},
                    user="default",
                    password="123",
                )
            except Exception as e:
                held["error"] = str(e)

        thread = threading.Thread(target=hold_the_learned_id)
        thread.start()
        try:
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                if (
                    node.http_query(
                        f"SELECT count() FROM system.processes WHERE query_id = '{first_id}'",
                        user="default",
                        password="123",
                    ).strip()
                    == "1"
                ):
                    break
                time.sleep(0.05)
            else:
                raise AssertionError(f"the holding query did not start: {held}")

            sock.sendall(_fe("Q", b"SELECT 'pg_second_statement_20250903'\x00"))
            types = read_until_ready(timeout=30.0)
            assert "E" not in types, types
            assert types[-1] == "Z", types
        finally:
            node.query(f"KILL QUERY WHERE query_id = '{first_id}' SYNC", password="123")
            thread.join()

    second_id = _pg_query_id_from_log(node, "pg_second_statement_20250903")
    assert second_id.startswith(f"postgres:{backend_key['pid']}:"), second_id
    assert second_id != first_id, (first_id, second_id)


def test_connection_ids_are_unique_across_listeners(started_cluster):
    """The connection id is the query id of every statement of its connection, and the id a
    CancelRequest resolves to, so it has to be unique across the whole server. Each endpoint is
    served by its own listener, so a counter held per listener would hand the same connection id,
    and with it the same query id and cancellation slot, to two live connections."""
    node = started_cluster.instances["node"]

    main_key = {}
    alt_key = {}
    sock_main, _ = _pg_raw_extended_query_session(node, main_key)
    with sock_main:
        sock_alt, _ = _pg_raw_extended_query_session(node, alt_key, port=alt_server_port)
        with sock_alt:
            assert main_key and alt_key, "the server did not send BackendKeyData"
            # The second connection is opened later, so a server-wide counter gives it a later id.
            assert alt_key["pid"] > main_key["pid"], (main_key, alt_key)


def test_cancel_request_with_wrong_secret_key_does_not_cancel(started_cluster):
    """The `BackendKeyData` secret is the whole credential a CancelRequest carries, so a request
    naming a live PostgreSQL query with the right process id and a wrong secret must not cancel it.
    Otherwise cancellation would be authenticated by the connection id alone, which is a small
    counter any client can enumerate."""
    node = started_cluster.instances["node"]

    backend_key = {}
    sock, read_until_ready = _pg_raw_extended_query_session(node, backend_key)
    with sock:
        assert backend_key, "the server did not send BackendKeyData"
        marker = "cancel_wrong_secret_20250903"

        def running():
            return node.http_query(
                "SELECT count() FROM system.processes"
                f" WHERE query LIKE '%{marker}%' AND query NOT LIKE '%system.processes%'",
                user="default",
                password="123",
            ).strip()

        def cancel(secret_key):
            with socket.create_connection((node.ip_address, server_port)) as cancel_sock:
                cancel_sock.sendall(
                    struct.pack("!iiII", 16, 80877102, backend_key["pid"], secret_key)
                )

        # Consume `sleepEachRow` so optimization cannot remove the delay. The statement sleeps for
        # 18 seconds of wall clock on every build, so it cannot end on its own within the checks.
        sock.sendall(
            _fe(
                "Q",
                b"SELECT '" + marker.encode() + b"', sum(sleepEachRow(0.3) + number) FROM numbers(60)"
                b" SETTINGS max_block_size = 1, max_threads = 1\x00",
            )
        )
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if running() == "1":
                break
            time.sleep(0.05)
        else:
            raise AssertionError("the PostgreSQL statement did not start running")

        # One bit off the genuine secret is not the genuine secret.
        cancel(backend_key["key"] ^ 1)
        time.sleep(2)
        assert running() == "1"

        # Control: the same request with the genuine secret does cancel, so the check above can fail.
        # The window is far shorter than the statement's remaining sleep, otherwise the statement
        # ending on its own would satisfy this too.
        cancel(backend_key["key"])
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            if running() == "0":
                break
            time.sleep(0.05)
        else:
            raise AssertionError("the genuine secret did not cancel the statement")

        # The client is told why, rather than being left to time out.
        assert "E" in read_until_ready(timeout=10.0)


def test_bind_portal_snapshots_statement(started_cluster):
    # The unnamed portal owns the statement snapshot captured by `Bind`.
    node = started_cluster.instances["node"]

    def sync():
        return _fe("S", b"")

    def close(kind, name):
        return _fe("C", kind.encode() + name.encode() + b"\x00")

    def parse(stmt, query, oids):
        b = stmt.encode() + b"\x00" + query.encode() + b"\x00" + struct.pack("!H", len(oids))
        for o in oids:
            b += struct.pack("!I", o)
        return _fe("P", b)

    def bind(portal, stmt, values):
        b = portal.encode() + b"\x00" + stmt.encode() + b"\x00" + struct.pack("!H", 0)
        b += struct.pack("!H", len(values))
        for v in values:
            vb = v.encode()
            b += struct.pack("!i", len(vb)) + vb
        b += struct.pack("!H", 0)
        return _fe("B", b)

    def execute(portal):
        return _fe("E", portal.encode() + b"\x00" + struct.pack("!I", 0))

    def datarow_values(raw):
        # Extract the first text column from each `DataRow` (`D`).
        out = []
        buf = raw
        while len(buf) >= 5:
            mtype = chr(buf[0])
            (mlen,) = struct.unpack("!I", buf[1:5])
            if len(buf) < 1 + mlen:
                break
            body = buf[5 : 1 + mlen]
            if mtype == "D":
                (ncols,) = struct.unpack("!H", body[0:2])
                if ncols >= 1:
                    (collen,) = struct.unpack("!i", body[2:6])
                    if collen >= 0:
                        out.append(body[6 : 6 + collen].decode())
            buf = buf[1 + mlen :]
        return out

    # Redefinition after `Bind` does not affect the portal.
    sock, read_until_ready = _pg_raw_extended_query_session(node)
    sock.settimeout(10)
    sock.sendall(
        parse("s", "SELECT 1", ())
        + bind("", "s", ())
        + parse("s", "SELECT 2", ())
        + execute("")
        + sync()
    )
    # Collect the whole reply up to ReadyForQuery.
    buf = b""
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        chunk = sock.recv(65536)
        if not chunk:
            break
        buf += chunk
        # Stop once we see a ReadyForQuery frame (Z, length 5) at the tail.
        if b"Z\x00\x00\x00\x05" in buf:
            break
    vals = datarow_values(buf)
    assert vals == ["1"], f"portal must run the bound SELECT 1, not the redefined SELECT 2, got {vals} ({buf!r})"
    assert b"E\x00" not in buf[:1] , "no error expected"
    sock.close()

    # Closing the statement after `Bind` does not invalidate the portal.
    sock, read_until_ready = _pg_raw_extended_query_session(node)
    sock.settimeout(10)
    sock.sendall(
        parse("s", "SELECT 1", ())
        + bind("", "s", ())
        + close("S", "s")
        + execute("")
        + sync()
    )
    buf = b""
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        chunk = sock.recv(65536)
        if not chunk:
            break
        buf += chunk
        if b"Z\x00\x00\x00\x05" in buf:
            break
    vals = datarow_values(buf)
    assert vals == ["1"], f"portal must survive Close of its prepared statement, got {vals} ({buf!r})"
    sock.close()


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


def test_catalog_qualifier_is_case_insensitive(started_cluster):
    """PostgreSQL folds unquoted identifiers to lower case, so a bare `PG_CATALOG`
    qualifier names the same schema as `pg_catalog` and must be stripped as well.
    A quoted identifier keeps its case in PostgreSQL, so `"PG_CATALOG"` is a
    different (and non-existent) schema and must not be rewritten."""
    node = started_cluster.instances["node"]

    ch = psycopg.connect(
        host=node.ip_address,
        port=server_port,
        user="default",
        password="123",
    )
    cur = ch.cursor()

    for qualifier in ["pg_catalog", "PG_CATALOG", "Pg_Catalog", '"pg_catalog"']:
        cur.execute(f"SELECT count() FROM {qualifier}.pg_namespace")
        assert int(cur.fetchall()[0][0]) > 0, qualifier

        cur.execute(f"SELECT {qualifier}.pg_table_is_visible(1)")
        assert str(cur.fetchall()[0][0]) in ("1", "True"), qualifier

    # A quoted qualifier in a different case is a different schema in PostgreSQL,
    # and there is no such database here.
    with pytest.raises(psycopg.errors.Error):
        cur.execute('SELECT count() FROM "PG_CATALOG".pg_namespace')

    ch.close()


def test_catalog_oids_are_unique(started_cluster):
    """The synthesized oids of the emulated catalog are used as join keys by
    PostgreSQL clients, so they have to be unique: `pg_class.relnamespace` must
    resolve to exactly one `pg_namespace` row, and no two relations may share
    an oid."""
    node = started_cluster.instances["node"]

    ch = psycopg.connect(
        host=node.ip_address,
        port=server_port,
        user="default",
        password="123",
    )
    cur = ch.cursor()
    cur.execute("CREATE DATABASE IF NOT EXISTS pg_oids_db")
    for i in range(16):
        cur.execute(f"CREATE DATABASE IF NOT EXISTS pg_oids_extra_{i}")
        cur.execute(
            f"CREATE TABLE IF NOT EXISTS pg_oids_db.t_{i} (id Int32) ENGINE = Memory"
        )
    ch.close()

    ch = psycopg.connect(
        host=node.ip_address,
        port=server_port,
        user="default",
        password="123",
        dbname="pg_oids_db",
    )
    cur = ch.cursor()

    cur.execute("SELECT oid, nspname FROM pg_namespace")
    namespaces = cur.fetchall()
    oids = [int(row[0]) for row in namespaces]
    assert len(oids) == len(set(oids))

    cur.execute("SELECT oid, relname FROM pg_class WHERE relname != ''")
    relations = cur.fetchall()
    relation_oids = [int(row[0]) for row in relations]
    assert len(relation_oids) == len(set(relation_oids))
    assert len(relations) == 16
    # The oid spaces of namespaces and relations must not overlap either.
    assert not (set(oids) & set(relation_oids))

    # The join psql performs behind `\d` must match exactly one namespace per relation.
    cur.execute(
        "SELECT c.relname, n.nspname FROM pg_class AS c "
        "JOIN pg_namespace AS n ON n.oid = c.relnamespace WHERE c.relname != ''"
    )
    joined = cur.fetchall()
    assert len(joined) == 16
    assert {row[1] for row in joined} == {"pg_oids_db"}

    ch.close()

    ch = psycopg.connect(
        host=node.ip_address,
        port=server_port,
        user="default",
        password="123",
    )
    cur = ch.cursor()
    cur.execute("DROP DATABASE IF EXISTS pg_oids_db")
    for i in range(16):
        cur.execute(f"DROP DATABASE IF EXISTS pg_oids_extra_{i}")
    ch.close()


def test_catalog_table_oids_differ_across_databases(started_cluster):
    """A session can switch the current database with `USE`, and `pg_class` then lists
    the tables of the new one. Two same-named tables in two databases are different
    objects, so their oids must differ - otherwise an oid a client remembered in the
    first database silently resolves to the other table after the switch."""
    node = started_cluster.instances["node"]

    databases = ["pg_oids_qualified_a", "pg_oids_qualified_b"]

    def connect(dbname=None):
        return psycopg.connect(
            host=node.ip_address,
            port=server_port,
            user="default",
            password="123",
            **({"dbname": dbname} if dbname else {}),
        )

    ch = connect()
    cur = ch.cursor()
    for database in databases:
        cur.execute(f"DROP DATABASE IF EXISTS {database}")
        cur.execute(f"CREATE DATABASE {database}")
        cur.execute(f"CREATE TABLE {database}.events (id Int32) ENGINE = Memory")
    ch.close()

    oids = []
    for database in databases:
        ch = connect(database)
        cur = ch.cursor()
        cur.execute("SELECT oid FROM pg_class WHERE relname = 'events'")
        oids.append(int(cur.fetchall()[0][0]))
        ch.close()

    assert oids[0] != oids[1]

    # The same, inside a single session that switches the database with `USE`:
    # the oid remembered before the switch must not name the other table after it.
    ch = connect(databases[0])
    cur = ch.cursor()
    cur.execute("SELECT oid FROM pg_class WHERE relname = 'events'")
    remembered = int(cur.fetchall()[0][0])
    cur.execute(f"USE {databases[1]}")
    cur.execute("SELECT oid FROM pg_class WHERE relname = 'events'")
    after_switch = int(cur.fetchall()[0][0])
    ch.close()

    assert remembered != after_switch
    assert {remembered, after_switch} == set(oids)

    ch = connect()
    cur = ch.cursor()
    for database in databases:
        cur.execute(f"DROP DATABASE IF EXISTS {database}")
    ch.close()


def test_catalog_oids_are_stable(started_cluster):
    """An oid identifies an object, and PostgreSQL clients are allowed to remember
    one and use it in a later query, so the oid of a database or a table must not
    change when unrelated objects appear."""
    node = started_cluster.instances["node"]

    ch = psycopg.connect(
        host=node.ip_address,
        port=server_port,
        user="default",
        password="123",
    )
    cur = ch.cursor()
    cur.execute("DROP DATABASE IF EXISTS pg_stable_oids_db")
    cur.execute("DROP DATABASE IF EXISTS pg_stable_oids_aaa")
    cur.execute("CREATE DATABASE pg_stable_oids_db")
    cur.execute("CREATE TABLE pg_stable_oids_db.zzz (id Int32) ENGINE = Memory")
    ch.close()

    def read_oids():
        ch = psycopg.connect(
            host=node.ip_address,
            port=server_port,
            user="default",
            password="123",
            dbname="pg_stable_oids_db",
        )
        cur = ch.cursor()
        cur.execute("SELECT oid FROM pg_namespace WHERE nspname = 'pg_stable_oids_db'")
        namespace_oid = int(cur.fetchall()[0][0])
        cur.execute("SELECT oid, relnamespace FROM pg_class WHERE relname = 'zzz'")
        row = cur.fetchall()[0]
        ch.close()
        return namespace_oid, int(row[0]), int(row[1])

    before = read_oids()

    ch = psycopg.connect(
        host=node.ip_address,
        port=server_port,
        user="default",
        password="123",
    )
    cur = ch.cursor()
    # Both names sort before the existing ones, which is what a scheme numbering
    # the objects by their position in the sorted list of names would shift.
    cur.execute("CREATE DATABASE pg_stable_oids_aaa")
    cur.execute("CREATE TABLE pg_stable_oids_db.aaa (id Int32) ENGINE = Memory")
    ch.close()

    assert read_oids() == before

    ch = psycopg.connect(
        host=node.ip_address,
        port=server_port,
        user="default",
        password="123",
    )
    cur = ch.cursor()
    cur.execute("DROP DATABASE IF EXISTS pg_stable_oids_db")
    cur.execute("DROP DATABASE IF EXISTS pg_stable_oids_aaa")
    ch.close()


def test_catalog_oids_do_not_depend_on_a_colliding_peer(started_cluster):
    """The oid of an object is a pure function of its name, so it must not change even
    when another name whose hash lands in the same slot appears or disappears. These two
    names are a real collision of the namespace oids: `sipHash64(name) % 2000000000`
    is 7242078 for both."""
    node = started_cluster.instances["node"]
    colliding = ["collision_probe_121841", "collision_probe_264544"]

    def sql(query, dbname=None):
        ch = psycopg.connect(
            host=node.ip_address,
            port=server_port,
            user="default",
            password="123",
            **({"dbname": dbname} if dbname else {}),
        )
        cur = ch.cursor()
        for statement in query:
            cur.execute(statement)
        rows = cur.fetchall() if cur.description else None
        ch.close()
        return rows

    sql([f"DROP DATABASE IF EXISTS {name}" for name in colliding])
    sql(
        [
            f"CREATE DATABASE {colliding[0]}",
            f"CREATE TABLE {colliding[0]}.{colliding[0]} (id Int32) ENGINE = Memory",
            f"CREATE TABLE {colliding[0]}.{colliding[1]} (id Int32) ENGINE = Memory",
        ]
    )

    def read_oids():
        namespace = sql(
            [f"SELECT oid FROM pg_namespace WHERE nspname = '{colliding[0]}'"],
            dbname=colliding[0],
        )
        relation = sql(
            [
                f"SELECT oid, relnamespace FROM pg_class WHERE relname = '{colliding[0]}'"
            ],
            dbname=colliding[0],
        )
        return int(namespace[0][0]), int(relation[0][0]), int(relation[0][1])

    # The first name is alone in its slot here - only its colliding peer as a table exists.
    before = read_oids()

    # Creating the colliding database must not renumber the object that is already there.
    sql([f"CREATE DATABASE {colliding[1]}"])
    assert read_oids() == before

    # Neither must dropping it again.
    sql([f"DROP DATABASE {colliding[1]}"])
    assert read_oids() == before

    # The accepted cost of that stability: while both colliding databases exist,
    # `pg_namespace` emits the same oid for both of them, so the join behind `\d`
    # cannot tell them apart. Uniqueness and stability are not both achievable in a
    # bounded oid space without a persistent oid counter, and stability wins - see the
    # comment above the view. This asserts the trade-off rather than a correct join.
    sql([f"CREATE DATABASE {colliding[1]}"])
    joined = sql(
        [
            "SELECT c.relname, n.nspname FROM pg_class AS c "
            "JOIN pg_namespace AS n ON n.oid = c.relnamespace "
            "WHERE c.relname != '' ORDER BY c.relname, n.nspname"
        ],
        dbname=colliding[0],
    )
    assert {row[0] for row in joined} == set(colliding)
    assert {row[1] for row in joined} == set(colliding)

    sql([f"DROP DATABASE IF EXISTS {name}" for name in colliding])
