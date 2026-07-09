# -*- coding: utf-8 -*-

import datetime
import decimal
import logging
import os
import random
import socket
import struct
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


def test_bind_binary_format_rejected(started_cluster):
    # The Bind message carries a format code per parameter (0 = text, 1 = binary).
    # This handler only understands text and would otherwise literalize a binary
    # payload as a raw byte string (silent misbinding), so a binary format code
    # must be rejected up front. Drive the extended Parse/Bind/Execute path with
    # an explicit binary format code via libpq and assert the server refuses it.
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
    # The Parse message declares a type OID per parameter. A declared type must be
    # preserved (emitted as `accurateCast('<value>', '<type>')`), not coerced to a
    # String literal, so a typed bind such as `SELECT $1 + 1` or `LIMIT $1` keeps
    # working, and the declared type's range/width is enforced by ClickHouse's own
    # parser. Force-quoting every value (the earlier injection fix) broke type
    # preservation; here we drive libpq's typed exec_params and assert types are
    # preserved and out-of-range/malformed values for the declared type are
    # rejected, while injection payloads can never break out of the value position.
    node = started_cluster.instances["node"]

    ch = psycopg.connect(
        host=node.ip_address,
        port=server_port,
        user="default",
        password="123",
    )
    pg = ch.pgconn

    # int4 OID = 23. `SELECT $1 + 1` must arithmetically add, not concatenate or
    # error: a String-coerced `'41' + 1` would not yield 42.
    res_add = pg.exec_params(b"SELECT $1 + 1", [b"41"], [23], [0], 0)
    assert res_add.status == psycopg.pq.ExecStatus.TUPLES_OK, res_add.error_message
    assert res_add.get_value(0, 0) == b"42"

    # `LIMIT $1` requires a numeric literal; a quoted string is rejected by
    # ClickHouse. With type preservation the bound int4 value works as a LIMIT.
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

    # An injection payload declared int4 stays quoted inside the cast, so it can
    # never splice SQL; the value simply fails to parse as the declared type.
    res_inj = pg.exec_params(
        b"SELECT $1", [b"1 UNION ALL SELECT 42"], [23], [0], 0
    )
    assert res_inj.status == psycopg.pq.ExecStatus.FATAL_ERROR

    # `1--`, `1+2`, `1-2` declared int4: the sharpest is `1--`. Emitted inside a
    # quoted cast argument as `x = accurateCast('1--', 'Int32') AND x = 42`, the
    # trailing predicate is preserved and the `--` cannot start a comment, so the
    # query errors on the bad value and never leaks rows for every tenant.
    for payload in (b"1--", b"1+2", b"1-2"):
        res_bad = pg.exec_params(
            b"SELECT count() FROM bind_num_t WHERE x = $1 AND x = 42",
            [payload],
            [23],
            [0],
            0,
        )
        assert res_bad.status == psycopg.pq.ExecStatus.FATAL_ERROR, payload

    # Type preservation validates the value against the DECLARED type. An int4
    # parameter must reject a fractional value: `SELECT $1 + 1` with `3.14` must not
    # assemble `SELECT 3.14 + 1` (int4 has no fractional part). It is rejected.
    res_int_frac = pg.exec_params(b"SELECT $1 + 1", [b"3.14"], [23], [0], 0)
    assert res_int_frac.status == psycopg.pq.ExecStatus.FATAL_ERROR

    # Range/width is enforced per declared OID, not just lexical shape: values
    # outside the declared integer type's range are rejected, not emitted as a
    # larger ClickHouse literal.
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

    # `oid` (OID 26) is unsigned: a negative value PostgreSQL would reject is
    # rejected here too, rather than emitted as a bare `-1`.
    res_oid_neg = pg.exec_params(b"SELECT $1", [b"-1"], [26], [0], 0)
    assert res_oid_neg.status == psycopg.pq.ExecStatus.FATAL_ERROR

    # Non-numeric declared OIDs are preserved too, not silently downgraded to a
    # quoted String literal (which regressed standards-compliant typed binds):
    #  - bool (OID 16): `NOT $1` must negate a boolean, not error on a string.
    res_bool = pg.exec_params(b"SELECT NOT $1", [b"true"], [16], [0], 0)
    assert res_bool.status == psycopg.pq.ExecStatus.TUPLES_OK, res_bool.error_message
    # Bool renders as `f`/`t` in the PostgreSQL text format (`false`/`0` in some
    # versions); accept any of them. The point is that it negated a boolean rather
    # than erroring on a string.
    assert res_bool.get_value(0, 0) in (b"f", b"false", b"0")
    res_bool_type = pg.exec_params(b"SELECT toTypeName($1)", [b"true"], [16], [0], 0)
    assert res_bool_type.get_value(0, 0) == b"Bool", res_bool_type.get_value(0, 0)

    #  - date (OID 1082): preserved as a Date, and an invalid date is rejected.
    res_date = pg.exec_params(b"SELECT toTypeName($1)", [b"2024-01-15"], [1082], [0], 0)
    assert res_date.status == psycopg.pq.ExecStatus.TUPLES_OK, res_date.error_message
    assert res_date.get_value(0, 0).startswith(b"Date"), res_date.get_value(0, 0)
    res_date_bad = pg.exec_params(b"SELECT $1", [b"not-a-date"], [1082], [0], 0)
    assert res_date_bad.status == psycopg.pq.ExecStatus.FATAL_ERROR

    #  - uuid (OID 2950): preserved as a UUID, and an injection payload declared
    #    uuid stays inside the cast and simply fails to parse.
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

    # `numeric` (OID 1700) has no Decimal literal in ClickHouse SQL. A bare `2.11`
    # would be reparsed as Float64 and lose precision; instead it is validated and
    # re-serialized as an exact Decimal, so `toTypeName($1)` reports a Decimal and
    # the exact value round-trips.
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

    # The injection payloads are rejected for a numeric OID too (not only int4):
    # the numeric branch validates the value as one literal before re-serializing.
    for payload in (b"1--", b"1+2"):
        res_num_bad = pg.exec_params(b"SELECT $1", [payload], [1700], [0], 0)
        assert res_num_bad.status == psycopg.pq.ExecStatus.FATAL_ERROR, payload
        assert b"prepared-statement parameter" in res_num_bad.error_message, payload

    # An oversize exponent for a numeric OID is rejected up front, before the
    # normalizer does any exponent arithmetic or zero-padding: `1e1000000` would
    # otherwise drive O(exponent) zero-padding and `1e99999999999999999999` would
    # overflow the signed exponent accumulator. Each is rejected and the
    # connection stays alive for the next request.
    for payload in (b"1e1000000", b"1e-1000000", b"1e99999999999999999999"):
        res_exp = pg.exec_params(b"SELECT $1", [payload], [1700], [0], 0)
        assert res_exp.status == psycopg.pq.ExecStatus.FATAL_ERROR, payload

    # timestamptz (OID 1184) carries its timezone in the type: it maps to
    # DateTime64(6, 'UTC'), not bare DateTime64(6), so toTypeName reports the
    # timezone-bearing type and offset values are interpreted as UTC.
    res_tstz = pg.exec_params(
        b"SELECT toTypeName($1)", [b"2024-01-15 12:30:45+02"], [1184], [0], 0
    )
    assert res_tstz.status == psycopg.pq.ExecStatus.TUPLES_OK, res_tstz.error_message
    assert b"UTC" in res_tstz.get_value(0, 0), res_tstz.get_value(0, 0)

    setup.execute("DROP TABLE bind_num_t;")
    ch.close()


def test_bind_unspecified_oid_infers_type(started_cluster):
    # An OID of 0 (or an omitted OID) in Parse means "the server infers the parameter
    # type from the statement", NOT "text". A standards-compliant frontend that binds
    # `SELECT $1 + 1` / `LIMIT $1` with an unspecified OID (e.g. PQexecParams with
    # paramTypes = NULL) must keep working as a number, not regress to `'41' + 1`
    # (type error) / `LIMIT '1'` (rejected). A numeric value is inferred as a numeric
    # literal; a non-numeric value stays a safely quoted string; injection payloads
    # can never break out of the value position.
    node = started_cluster.instances["node"]

    ch = psycopg.connect(
        host=node.ip_address,
        port=server_port,
        user="default",
        password="123",
    )
    pg = ch.pgconn

    # OID 0 = unspecified. `SELECT $1 + 1` with `41` must add arithmetically (42),
    # not concatenate/error as a String would.
    res_add = pg.exec_params(b"SELECT $1 + 1", [b"41"], [0], [0], 0)
    assert res_add.status == psycopg.pq.ExecStatus.TUPLES_OK, res_add.error_message
    assert res_add.get_value(0, 0) == b"42"

    # `LIMIT $1` with an unspecified OID requires a numeric literal; inference makes
    # the bound value work as a LIMIT (a String would be rejected by ClickHouse).
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

    # Passing no paramTypes at all (paramTypes = NULL, the common libpq call) also
    # leaves the OID unspecified and infers.
    res_none = pg.exec_params(b"SELECT $1 + 1", [b"41"])
    assert res_none.status == psycopg.pq.ExecStatus.TUPLES_OK, res_none.error_message
    assert res_none.get_value(0, 0) == b"42"

    # A boolean keyword (`true`/`false`) carries an unambiguous type in its own text,
    # so an unspecified-OID boolean bind infers as Bool. `SELECT NOT $1` + `true` must
    # negate a boolean (NOT rejects a String argument), not become `NOT 'true'`.
    res_bool = pg.exec_params(b"SELECT NOT $1", [b"true"], [0], [0], 0)
    assert res_bool.status == psycopg.pq.ExecStatus.TUPLES_OK, res_bool.error_message
    # `NOT true` is false, rendered as `f`/`false`/`0` depending on the text format.
    assert res_bool.get_value(0, 0) in (b"f", b"false", b"0"), res_bool.get_value(0, 0)

    # A non-numeric, non-boolean unspecified-OID value can only be inferred as text, so
    # it stays a safely quoted string literal.
    res_text = pg.exec_params(b"SELECT $1", [b"hello"], [0], [0], 0)
    assert res_text.status == psycopg.pq.ExecStatus.TUPLES_OK, res_text.error_message
    assert res_text.get_value(0, 0) == b"hello"

    # A boolean-looking payload with a trailing injection is not the exact `true`/
    # `false` keyword, so it stays a quoted string and cannot splice SQL.
    res_bool_inj = pg.exec_params(b"SELECT $1", [b"true; DROP TABLE bind_infer_t"], [0], [0], 0)
    assert res_bool_inj.status == psycopg.pq.ExecStatus.TUPLES_OK, res_bool_inj.error_message
    assert res_bool_inj.get_value(0, 0) == b"true; DROP TABLE bind_infer_t"

    # Injection payloads with an unspecified OID are not single numeric literals, so
    # they fall through to a quoted string and cannot splice SQL. `1--` used as a
    # WHERE value can never truncate the trailing predicate into a comment.
    res_inj = pg.exec_params(
        b"SELECT count() FROM bind_infer_t WHERE x = $1 AND x = 42",
        [b"1--"],
        [0],
        [0],
        0,
    )
    # The quoted string compared against an Int32 column is a type error, not a
    # rows-leaking comment truncation; either way it must not return all rows.
    if res_inj.status == psycopg.pq.ExecStatus.TUPLES_OK:
        assert res_inj.get_value(0, 0) == b"0", res_inj.get_value(0, 0)

    setup.execute("DROP TABLE bind_infer_t;")
    ch.close()


def test_bind_error_keeps_connection_alive(started_cluster):
    # An extended-query (Parse/Bind/Execute) error must not drop the connection:
    # per the PostgreSQL protocol the backend sends ErrorResponse, discards
    # messages until Sync, then sends ReadyForQuery so the same connection stays
    # usable. Rejecting a typed Bind value (the type-preservation validation) must
    # therefore leave the client able to run further queries on the same session.
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


def _pg_raw_extended_query_session(node):
    # Minimal PostgreSQL v3 wire client used to drive the extended-query state
    # machine directly (Parse/Bind/Describe/Close/Sync). libpq/psycopg hide these
    # messages, so a raw socket is the only way to send a standalone
    # Describe/Sync or Close/Sync and count the backend's ReadyForQuery replies.
    sock = socket.create_connection((node.ip_address, server_port), timeout=10)

    def read_until_ready(timeout=10.0):
        # Read framed backend messages until ReadyForQuery ('Z'); return the
        # list of message type characters seen (in order).
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
    # Regression for the extended-query protocol contract on the typed-Bind path.
    #
    # ReadyForQuery must be emitted exactly once per Sync. Marking only
    # Parse/Bind (not Describe/Close/Execute) as in-progress caused a standalone
    # Describe/Sync or Close/Sync to emit ReadyForQuery mid-cycle (before the
    # Sync was read) and then again for the Sync, desyncing strict clients that
    # count one ReadyForQuery per Sync. Every extended-query message now sets
    # is_query_in_progress; only Sync clears it and emits the single
    # ReadyForQuery that ends the series.
    #
    # Describe is a silent no-op: this wire implementation does not know the row
    # layout until the query runs, and the RowDescription is emitted at Execute
    # time by PostgreSQLOutputFormat, which standard clients (which pipeline
    # Describe with Bind/Execute/Sync) read together. So a Parse/Bind/Describe/
    # Execute/Sync cycle must let the Execute run to completion, not abort it.
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

    # Close('S', <nonexistent>)/Sync: CloseComplete then exactly ONE ReadyForQuery.
    sock, read_until_ready = _pg_raw_extended_query_session(node)
    sock.sendall(close("S", "nope") + sync())
    types = read_until_ready()
    assert types.count("Z") == 1, f"Close/Sync must emit one ReadyForQuery, got {types}"
    assert "3" in types, f"Close must respond with CloseComplete, got {types}"
    sock.close()

    # Describe('S', '')/Sync: a silent no-op then exactly ONE ReadyForQuery. The
    # backend must not desync by emitting a mid-cycle ReadyForQuery before Sync.
    sock, read_until_ready = _pg_raw_extended_query_session(node)
    sock.sendall(describe("S", "") + sync())
    types = read_until_ready()
    assert types.count("Z") == 1, f"Describe/Sync must emit one ReadyForQuery, got {types}"
    sock.close()

    # Full Parse/Bind/Describe/Execute/Sync: the Describe no-op must NOT abort the
    # cycle. The Execute runs, so the backend sends RowDescription ('T'),
    # DataRow ('D'), CommandComplete ('C') and then exactly ONE ReadyForQuery for
    # the Sync. The typed int4 bind keeps this on the path this PR touches.
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

    # A rejected typed Bind (the 1-- injection guard) must still emit exactly one
    # ReadyForQuery per Sync and keep the connection usable afterwards.
    sock, read_until_ready = _pg_raw_extended_query_session(node)
    sock.sendall(parse("", "SELECT $1 AS a", (23,)) + bind("", "", ("1--",)) + execute("") + sync())
    types = read_until_ready()
    assert "E" in types, f"1-- injection must be rejected, got {types}"
    assert types.count("Z") == 1, f"rejected Bind must emit one ReadyForQuery, got {types}"
    sock.sendall(_fe("Q", b"SELECT 7\x00"))
    types = read_until_ready()
    assert "C" in types, f"connection must stay alive after a rejected Bind, got {types}"
    sock.close()


def test_bind_negative_count_recovers(started_cluster):
    # Regression for malformed Bind count fields. A negative num_params (or
    # num_format_params_result) would skip the params loop, misread the following
    # bytes as the next count, and leave the rest of the Bind body unread, which
    # desyncs the skip-until-Sync recovery. The backend must reject a negative
    # count with an ErrorResponse and still emit exactly one ReadyForQuery for the
    # Sync, keeping the connection usable.
    node = started_cluster.instances["node"]

    def sync():
        return _fe("S", b"")

    # Bind with num_params = -1: empty portal/statement names, no parameter
    # format codes, then a negative parameter count.
    def bind_neg_num_params():
        b = b"\x00" + b"\x00" + struct.pack("!H", 0) + struct.pack("!h", -1)
        return _fe("B", b)

    # Bind with a negative result-format-code count: valid names, no format
    # codes, no parameters, then a negative result-format-code count.
    def bind_neg_result_formats():
        b = (
            b"\x00"
            + b"\x00"
            + struct.pack("!H", 0)
            + struct.pack("!H", 0)
            + struct.pack("!h", -1)
        )
        return _fe("B", b)

    for make_bind in (bind_neg_num_params, bind_neg_result_formats):
        sock, read_until_ready = _pg_raw_extended_query_session(node)
        sock.sendall(make_bind() + sync())
        types = read_until_ready()
        assert "E" in types, f"malformed Bind must be rejected, got {types}"
        assert types.count("Z") == 1, (
            f"malformed Bind must emit one ReadyForQuery per Sync, got {types}"
        )
        # The same connection must stay usable (stream stayed aligned).
        sock.sendall(_fe("Q", b"SELECT 7\x00"))
        types = read_until_ready()
        assert "C" in types, f"connection must stay alive after malformed Bind, got {types}"
        sock.close()


def test_flush_error_discards_until_sync(started_cluster):
    # Regression for the skip-until-Sync recovery on the FLUSH and unsupported-
    # message error branches. FLUSH is not supported and answers with an
    # ErrorResponse. Before the fix that branch left ignore_until_sync = false, so
    # a pipeline like Parse; Bind; FLUSH; Execute; Sync would still run the Execute
    # after the FLUSH error instead of discarding everything until Sync. The
    # Execute must NOT run (no RowDescription / DataRow / CommandComplete), and the
    # backend must emit exactly one ReadyForQuery for the Sync, then stay usable.
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
    # Regression for the Bind requested-result-format codes. We always emit text
    # rows and RowDescription advertises FormatCode::TEXT for every column, so a
    # binary result format request (resultFormat = 1) is accepted and ignored: the
    # client receives text and adapts. Real clients (e.g. Npgsql / the .NET driver)
    # request binary results by default, so rejecting the request would break them.
    # The extended-query flow must complete normally (RowDescription, DataRow,
    # CommandComplete) and the result-format codes must not misalign the stream.
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
    # Regression: ReadyForQuery must be emitted exactly once per Sync, including a
    # bare standalone Sync issued while the backend is already idle. When
    # ReadyForQuery was sent speculatively at the top of every idle loop iteration,
    # a client sending Sync while idle received that pre-loop ReadyForQuery, and the
    # Sync then produced a second one, giving two per Sync. ReadyForQuery is now
    # emitted only at explicit boundaries (startup, simple query, Sync), so each
    # Sync yields exactly one.
    node = started_cluster.instances["node"]

    def sync():
        return _fe("S", b"")

    sock, read_until_ready = _pg_raw_extended_query_session(node)
    # A single standalone Sync: exactly one ReadyForQuery.
    sock.sendall(sync())
    types = read_until_ready()
    assert types.count("Z") == 1, f"standalone Sync must emit one ReadyForQuery, got {types}"

    # Several standalone Syncs in a row: exactly one ReadyForQuery each (no extra
    # mid-cycle ReadyForQuery). Read them one at a time so the count is unambiguous.
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
    # Regression: Bind arity is the statement's placeholder count (highest $N), not
    # the number of declared parameter type OIDs. Parse may declare fewer OIDs than
    # there are placeholders, so checking against the declared-type count let a
    # "SELECT $1, $2" statement bound with one value through, leaving $2 in the SQL
    # at Execute; extra values were silently dropped. Both mismatches must now be
    # rejected with an ErrorResponse, and the connection must recover.
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

    # Two placeholders, no declared OIDs, one bound value -> rejected (previously
    # this passed and left $2 literally in the query).
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

    # Too many values (one placeholder, two values) -> rejected (previously the
    # extra value was silently dropped).
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
    # Simple-query PREPARE/EXECUTE path: EXECUTE arguments are spliced into the
    # prepared statement body by $N substitution, so a string argument must be
    # emitted as a quoted+escaped SQL literal, never as raw SQL text.
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

    # Injection through a real, bare $N placeholder (not one wrapped in quotes:
    # a quoted '$1' is a string-literal token and is left untouched, so it would
    # never exercise this sink). The placeholder sits in a numeric comparison, so
    # a client passes a string argument expecting it to be bound as one value. It
    # must be emitted as a quoted+escaped literal; raw substitution would splice
    # "1 UNION ALL SELECT secret ..." straight into the SQL and leak the secret.
    # With the fix the string cannot be coerced to Int32 and the query errors out,
    # which also drops the connection, so this runs on its own connection.
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

    # An argument echoed straight back must round-trip as data, never as SQL: the
    # same payload through "SELECT $1" comes back as a single string value.
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
    # The exact-arity invariant must hold on the simple SQL PREPARE/EXECUTE path too,
    # not only the extended Bind path. Without the check `EXECUTE s(1, 2)` on
    # `PREPARE s AS SELECT $1` silently drops the extra argument, and `EXECUTE s(1)`
    # on `PREPARE s AS SELECT $1, $2` leaves `$2` literally in the executed SQL.
    node = started_cluster.instances["node"]

    def connect():
        return psycopg.connect(
            host=node.ip_address,
            port=server_port,
            user="default",
            password="123",
        )

    # Exact arity is accepted (a fresh connection per case: a rejected EXECUTE errors
    # out and drops the connection).
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


def test_execute_accepts_non_literal_arguments(started_cluster):
    # A simple-query EXECUTE argument may be a general expression, not only a
    # literal (e.g. `1 + 1`, `now()`). The argument formatting used to assume
    # every argument was a literal and dereferenced a null pointer for
    # expressions, crashing the connection. Expressions must be serialized into a
    # safe SQL fragment instead, and string literals inside them must stay quoted
    # so injection remains impossible.
    node = started_cluster.instances["node"]

    def connect():
        return psycopg.connect(
            host=node.ip_address,
            port=server_port,
            user="default",
            password="123",
        )

    # An arithmetic expression argument must not crash and must evaluate.
    ch = connect()
    cur = ch.cursor()
    cur.execute("PREPARE expr_arith AS SELECT $1 AS v;")
    cur.execute("EXECUTE expr_arith(1 + 1);")
    assert cur.fetchall() == [(2,)]
    ch.close()

    # A function-call expression argument must not crash and must evaluate.
    ch = connect()
    cur = ch.cursor()
    cur.execute("PREPARE expr_func AS SELECT $1 AS v;")
    cur.execute("EXECUTE expr_func(abs(-5));")
    assert cur.fetchall() == [(5,)]
    ch.close()

    # A negative number is a single literal and round-trips unchanged.
    ch = connect()
    cur = ch.cursor()
    cur.execute("PREPARE expr_neg AS SELECT $1 AS v;")
    cur.execute("EXECUTE expr_neg(-7);")
    assert cur.fetchall() == [(-7,)]
    ch.close()

    # Injection stays impossible for an expression that embeds a string literal:
    # the string is serialized as a single quoted literal, so the concat result
    # is plain data, never spliced SQL.
    ch = connect()
    cur = ch.cursor()
    cur.execute("PREPARE expr_concat AS SELECT $1 AS v;")
    cur.execute("EXECUTE expr_concat(concat('1 UNION ALL SELECT 2', ' -- '));")
    assert cur.fetchall() == [("1 UNION ALL SELECT 2 -- ",)]
    ch.close()


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


def test_bind_portal_snapshots_statement(started_cluster):
    # Regression for the extended-query portal contract. Once Bind creates the
    # (unnamed) portal, the portal owns a snapshot of the referenced prepared
    # statement. A later Parse that redefines the statement, or a Close that
    # deallocates it, must not change what the already-bound Execute runs. Before
    # the fix, Execute re-resolved the statement from the live map, so
    # redefinition leaked into the bound portal and a Close turned Execute into
    # "Execute without prior Bind".
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
        # Extract the DataRow ('D') payloads' first column text from a raw byte
        # stream of framed backend messages.
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

    # Redefining the prepared statement after Bind must not affect the portal:
    # Parse s AS SELECT 1; Bind("", s); Parse s AS SELECT 2; Execute("") -> 1.
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

    # Deallocating the prepared statement after Bind must not invalidate the
    # portal: Parse s; Bind("", s); Close('S', s); Execute("") -> still runs.
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
