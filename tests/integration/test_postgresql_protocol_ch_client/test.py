# Verifies that the `postgresql` table function and the `PostgreSQL` table engine can talk to a
# ClickHouse server through its own PostgreSQL wire protocol port (issue #52639). ClickHouse acts as
# a libpq/pqxx client against itself: it opens a read-only transaction and introspects the emulated
# `pg_catalog` tables (`pg_namespace`, `pg_class`, `pg_attribute`, `format_type`, `current_setting`)
# to discover the table structure, then reads the data.

import io

import psycopg2 as py_psql
import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/postgresql.xml"],
    user_configs=["configs/users.xml"],
)

PG_PORT = 5433


def pg_source(database, table):
    return f"postgresql('127.0.0.1:{PG_PORT}', '{database}', '{table}', 'pguser', 'pgpass')"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_issue_52639_select_from_system_one(started_cluster):
    # The exact query from the issue: connecting to a ClickHouse instance over the PostgreSQL protocol.
    assert node.query(f"SELECT 1 FROM {pg_source('system', 'one')}") == "1\n"


def test_read_table_via_postgresql_function(started_cluster):
    node.query("DROP TABLE IF EXISTS test_self SYNC")
    node.query(
        "CREATE TABLE test_self (a UInt32, b String, c Nullable(Int64), d Float64) "
        "ENGINE = MergeTree ORDER BY a"
    )
    node.query(
        "INSERT INTO test_self VALUES (1, 'one', 10, 1.5), (2, 'two', NULL, 2.5), (3, 'three', 30, 3.5)"
    )

    # No schema qualifier: the table is resolved in the connected ('default') database via the 'public' schema.
    assert (
        node.query(f"SELECT a, b, c, d FROM {pg_source('default', 'test_self')} ORDER BY a")
        == "1\tone\t10\t1.5\n2\ttwo\t\\N\t2.5\n3\tthree\t30\t3.5\n"
    )

    assert node.query(f"SELECT count() FROM {pg_source('default', 'test_self')}") == "3\n"


def test_postgresql_table_engine(started_cluster):
    node.query("DROP TABLE IF EXISTS test_engine_src SYNC")
    node.query("DROP TABLE IF EXISTS test_engine SYNC")
    node.query("CREATE TABLE test_engine_src (id UInt64, s String) ENGINE = MergeTree ORDER BY id")
    node.query("INSERT INTO test_engine_src VALUES (100, 'x'), (200, 'y')")

    node.query(
        "CREATE TABLE test_engine (id UInt64, s String) "
        f"ENGINE = PostgreSQL('127.0.0.1:{PG_PORT}', 'default', 'test_engine_src', 'pguser', 'pgpass')"
    )
    assert node.query("SELECT id, s FROM test_engine ORDER BY id") == "100\tx\n200\ty\n"


def test_wide_and_decimal_type_roundtrip(started_cluster):
    # PostgreSQL has neither unsigned nor >64-bit integers. Types that do not fit into a signed 64-bit
    # `bigint` (UInt64 and the 128/256-bit integer types) must be advertised through the emulated catalog
    # as `numeric` with a precision wide enough to hold every value - not as `bigint`, which would reject
    # values above the Int64 range - and `Decimal` must keep its precision and scale via a real
    # `atttypmod` so schema inference does not collapse it to a bare `numeric`.
    node.query("DROP TABLE IF EXISTS test_types SYNC")
    node.query(
        "CREATE TABLE test_types ("
        "  u64 UInt64, i128 Int128, u128 UInt128, i256 Int256, "
        "  d55 Decimal(5, 5), d3810 Decimal(38, 10)"
        ") ENGINE = MergeTree ORDER BY u64"
    )
    node.query(
        "INSERT INTO test_types VALUES ("
        "18446744073709551615, "  # UInt64 max, above the Int64 range
        "170141183460469231731687303715884105727, "  # Int128 max
        "340282366920938463463374607431768211455, "  # UInt128 max
        "57896044618658097711785492504343953926634992332820282019728792003956564819967, "  # Int256 max
        "0.12345, "
        "1234567890123456789012345678.1234567890)"
    )

    assert node.query(
        "SELECT u64, i128, u128, i256, d55, d3810 "
        f"FROM {pg_source('default', 'test_types')}"
    ) == (
        "18446744073709551615\t"
        "170141183460469231731687303715884105727\t"
        "340282366920938463463374607431768211455\t"
        "57896044618658097711785492504343953926634992332820282019728792003956564819967\t"
        "0.12345\t"
        "1234567890123456789012345678.1234567890\n"
    )


def test_copy_to_stdout_csv_multiline_value(started_cluster):
    # `COPY (query) TO STDOUT WITH FORMAT csv` must stream a value that itself contains a newline intact:
    # each row is serialized into its own CopyData message, so a quoted CSV field spanning several physical
    # lines is not chopped up the way the previous newline-based splitting of a whole block would have done.
    conn = py_psql.connect(
        host=node.ip_address, port=PG_PORT, user="pguser", password="pgpass", database="default"
    )
    try:
        cur = conn.cursor()
        out = io.StringIO()
        cur.copy_expert("COPY (SELECT 'line1\nline2' AS s, 7 AS n) TO STDOUT WITH FORMAT csv", out)
        assert out.getvalue() == '"line1\nline2",7\n'
    finally:
        conn.close()


def test_explicit_schema(started_cluster):
    # A ClickHouse database can be addressed as a PostgreSQL schema (the 6th argument of the function):
    # here we connect to the 'default' database but read 'system.one' via the explicit 'system' schema.
    assert (
        node.query(
            f"SELECT dummy FROM postgresql('127.0.0.1:{PG_PORT}', 'default', 'one', 'pguser', 'pgpass', 'system')"
        )
        == "0\n"
    )
