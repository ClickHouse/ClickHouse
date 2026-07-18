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
        "1234567890123456789012345678.1234567891)"
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
        "1234567890123456789012345678.1234567891\n"
    )


def test_array_type_roundtrip(started_cluster):
    # `postgresql(..., 'arr_table')` against a ClickHouse table with array columns must infer the array
    # element types and dimensions (not fall back to String) and read the values back. The emulated
    # `pg_attribute` advertises the element OID plus `attndims`, and the server streams the values in
    # PostgreSQL array-literal form (`{...}`) so `pqxx::array_parser` on the reading side can parse them.
    node.query("DROP TABLE IF EXISTS test_arrays SYNC")
    node.query(
        "CREATE TABLE test_arrays "
        "(id UInt32, ai Array(Int32), astr Array(String), aai Array(Array(Int32)), au Array(UInt64)) "
        "ENGINE = MergeTree ORDER BY id"
    )
    node.query(
        "INSERT INTO test_arrays VALUES "
        "(1, [1, 2, 3], ['a', 'b'], [[1, 2], [3, 4]], [10, 20]), "
        "(2, [], ['x'], [[9]], [18446744073709551615])"
    )

    # The types are inferred as arrays (a UInt64 element becomes numeric(20, 0) -> Decimal(20, 0)), not String.
    assert node.query(
        "SELECT toTypeName(ai), toTypeName(astr), toTypeName(aai), toTypeName(au) "
        f"FROM {pg_source('default', 'test_arrays')} LIMIT 1"
    ) == "Array(Int32)\tArray(String)\tArray(Array(Int32))\tArray(Decimal(20, 0))\n"

    # The values round-trip, including nested and empty arrays.
    assert node.query(
        f"SELECT id, ai, astr, aai, au FROM {pg_source('default', 'test_arrays')} ORDER BY id"
    ) == (
        "1\t[1,2,3]\t['a','b']\t[[1,2],[3,4]]\t[10,20]\n"
        "2\t[]\t['x']\t[[9]]\t[18446744073709551615]\n"
    )


def test_array_of_decimal_roundtrip(started_cluster):
    # `system.columns.numeric_precision` / `numeric_scale` are NULL for an `Array(Decimal(p, s))` column
    # (they are only populated for top-level numeric types), so the emulated `pg_attribute` must recover
    # the element precision and scale from the type name itself; otherwise the column would be advertised
    # as a bare `numeric[]` and inferred back as `Array(Decimal(38, 19))`.
    node.query("DROP TABLE IF EXISTS test_dec_arrays SYNC")
    node.query(
        "CREATE TABLE test_dec_arrays "
        "(id UInt32, ad Array(Decimal(5, 2)), awide Array(Decimal(38, 10))) "
        "ENGINE = MergeTree ORDER BY id"
    )
    node.query(
        "INSERT INTO test_dec_arrays VALUES "
        "(1, [1.25, -3.57], [1234567890123456789012345678.1234567891])"
    )

    assert node.query(
        "SELECT toTypeName(ad), toTypeName(awide) "
        f"FROM {pg_source('default', 'test_dec_arrays')} LIMIT 1"
    ) == "Array(Decimal(5, 2))\tArray(Decimal(38, 10))\n"

    assert node.query(
        f"SELECT ad, awide FROM {pg_source('default', 'test_dec_arrays')}"
    ) == "[1.25,-3.57]\t[1234567890123456789012345678.1234567891]\n"


def test_array_of_nullable_roundtrip(started_cluster):
    # An `Array(Nullable(T))` column must be inferred with a nullable element type so that a NULL element is
    # read back as NULL instead of being rewritten to the element type's default. The column type does not
    # start with `Nullable(`, so the emulated `pg_attribute` has to detect the nullable element from the
    # `Nullable(` wrapper inside the leading `Array(` chain and advertise `attnotnull = 'f'`.
    node.query("DROP TABLE IF EXISTS test_nullable_arrays SYNC")
    node.query(
        "CREATE TABLE test_nullable_arrays "
        "(id UInt32, an Array(Nullable(Int32)), asn Array(Nullable(String))) "
        "ENGINE = MergeTree ORDER BY id"
    )
    node.query(
        "INSERT INTO test_nullable_arrays VALUES (1, [1, NULL, 3], ['a', NULL, 'c'])"
    )

    assert node.query(
        "SELECT toTypeName(an), toTypeName(asn) "
        f"FROM {pg_source('default', 'test_nullable_arrays')} LIMIT 1"
    ) == "Array(Nullable(Int32))\tArray(Nullable(String))\n"

    # The NULL elements survive the round-trip (they are not turned into 0 / '').
    assert node.query(
        f"SELECT id, an, asn FROM {pg_source('default', 'test_nullable_arrays')} ORDER BY id"
    ) == "1\t[1,NULL,3]\t['a',NULL,'c']\n"


def test_select_constant_array_over_wire(started_cluster):
    # A constant array expression must be streamed in PostgreSQL array-literal form like a table-backed
    # one: the array serializer has to cope with a `ColumnConst` input (`SELECT [1, 2]` produces one when
    # the caller does not materialize its input) instead of assuming a materialized `ColumnArray`.
    conn = py_psql.connect(
        host=node.ip_address, port=PG_PORT, user="pguser", password="pgpass", database="default"
    )
    try:
        cur = conn.cursor()
        cur.execute("SELECT [1, 2] AS a, ['x', 'y'] AS s")
        # Arrays are advertised as text in the RowDescription of a direct SELECT, so the client sees
        # the PostgreSQL literal itself (every scalar element is quoted).
        assert cur.fetchall() == [('{"1","2"}', '{"x","y"}')]
    finally:
        conn.close()


def test_copy_to_stdout_binary_is_rejected(started_cluster):
    # PostgreSQL binary `COPY` has its own wire format (a `PGCOPY` header and per-field length framing)
    # that ClickHouse does not implement, and the wire path always advertises the text format code.
    # `WITH FORMAT binary` must be rejected with a clear error rather than returning an incompatible
    # payload. The check is case-insensitive, so the upper-case spelling is rejected too.
    conn = py_psql.connect(
        host=node.ip_address, port=PG_PORT, user="pguser", password="pgpass", database="default"
    )
    try:
        with pytest.raises(py_psql.Error, match="binary COPY format is not supported"):
            cur = conn.cursor()
            out = io.BytesIO()
            cur.copy_expert("COPY (SELECT [1, 2, 3] AS a, 7 AS n) TO STDOUT WITH FORMAT BINARY", out)
    finally:
        conn.close()


def test_copy_to_stdout_format_is_case_insensitive(started_cluster):
    # PostgreSQL keywords are case-insensitive, so the `COPY ... WITH FORMAT <name>` format name must be
    # matched irrespective of case. `FORMAT TSV` must produce tab-separated output (not CSV): a previous
    # bug compared the original spelling - so upper-case `FORMAT CSV` was rejected - and mapped `tsv` to
    # the CSV format.
    conn = py_psql.connect(
        host=node.ip_address, port=PG_PORT, user="pguser", password="pgpass", database="default"
    )
    try:
        cur = conn.cursor()

        out = io.StringIO()
        cur.copy_expert("COPY (SELECT 1 AS a, 2 AS b) TO STDOUT WITH FORMAT CSV", out)
        assert out.getvalue() == "1,2\n"

        out = io.StringIO()
        cur.copy_expert("COPY (SELECT 1 AS a, 2 AS b) TO STDOUT WITH FORMAT TSV", out)
        assert out.getvalue() == "1\t2\n"

        out = io.StringIO()
        cur.copy_expert("COPY (SELECT 1 AS a, 2 AS b) TO STDOUT WITH FORMAT tsv", out)
        assert out.getvalue() == "1\t2\n"
    finally:
        conn.close()


def test_map_and_tuple_columns_are_not_advertised_as_arrays(started_cluster):
    # An `Array(...)` nested inside a `Map`/`Tuple` type argument must not make the emulated `pg_attribute`
    # advertise the column as a PostgreSQL array: only the leading `Array(` wrappers count towards
    # `attndims`. Such columns are exposed as text and read back as `String` (their ClickHouse text form),
    # while a genuine top-level array in the same table is still inferred as an array.
    node.query("DROP TABLE IF EXISTS test_nested_containers SYNC")
    node.query(
        "CREATE TABLE test_nested_containers "
        "(id UInt32, m Map(String, Array(UInt8)), t Tuple(a Array(Int32)), arr Array(Int32)) "
        "ENGINE = MergeTree ORDER BY id"
    )
    node.query(
        "INSERT INTO test_nested_containers VALUES "
        "(1, map('k', [1, 2]), tuple([3, 4]), [5, 6])"
    )

    assert node.query(
        "SELECT toTypeName(m), toTypeName(t), toTypeName(arr) "
        f"FROM {pg_source('default', 'test_nested_containers')} LIMIT 1"
    ) == "String\tString\tArray(Int32)\n"

    # The single quotes inside the `String` value are backslash-escaped by the TSV output format.
    assert node.query(
        f"SELECT id, m, t, arr FROM {pg_source('default', 'test_nested_containers')} ORDER BY id"
    ) == "1\t{\\'k\\':[1,2]}\t([3,4])\t[5,6]\n"


def test_wire_types_for_wide_and_decimal(started_cluster):
    # A direct PostgreSQL client reading these columns over the wire must see the correct type OIDs in the
    # `RowDescription`: the integer types that do not fit into a signed 64-bit `bigint` (UInt64 and the
    # 128/256-bit types) and the `Decimal` types are advertised as `numeric` (OID 1700), not `varchar`
    # (OID 1043). `Int64`, which fits into `bigint`, keeps OID 20.
    node.query("DROP TABLE IF EXISTS test_wire_types SYNC")
    node.query(
        "CREATE TABLE test_wire_types (i64 Int64, u64 UInt64, i128 Int128, d Decimal(10, 2)) "
        "ENGINE = MergeTree ORDER BY i64"
    )
    node.query("INSERT INTO test_wire_types VALUES (1, 2, 3, 4.5)")

    conn = py_psql.connect(
        host=node.ip_address, port=PG_PORT, user="pguser", password="pgpass", database="default"
    )
    try:
        cur = conn.cursor()
        cur.execute("SELECT i64, u64, i128, d FROM test_wire_types")
        # 20 = int8 (bigint), 1700 = numeric.
        assert [c.type_code for c in cur.description] == [20, 1700, 1700, 1700]
    finally:
        conn.close()


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
