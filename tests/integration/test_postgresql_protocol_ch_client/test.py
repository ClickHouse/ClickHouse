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


def test_uint256_is_recovered_as_int256_or_rejected(started_cluster):
    # A self-connected `UInt256` is advertised as `numeric(78, 0)` (the smallest `numeric` that holds every
    # 256-bit integer), and `convertPostgreSQLDataType` maps any `numeric(p > 76, 0)` back to `Int256` -
    # PostgreSQL `numeric` is signed, so there is no unsigned counterpart and this mapping is shared with
    # real PostgreSQL sources (issue #59224). A `UInt256` value within the `Int256` range therefore reads
    # back as `Int256`, and a value above the `Int256` maximum is rejected (fail-closed) instead of being
    # silently wrapped around, matching the `numeric -> Int256` overflow check.
    node.query("DROP TABLE IF EXISTS test_u256 SYNC")
    node.query("CREATE TABLE test_u256 (x UInt256) ENGINE = MergeTree ORDER BY x")

    # A value within the Int256 range round-trips (recovered as Int256).
    in_range = "57896044618658097711785492504343953926634992332820282019728792003956564819967"  # Int256 max
    node.query(f"INSERT INTO test_u256 VALUES ({in_range})")
    assert node.query(f"SELECT x FROM {pg_source('default', 'test_u256')}") == in_range + "\n"
    assert node.query(f"SELECT toTypeName(x) FROM {pg_source('default', 'test_u256')}") == "Int256\n"

    # A value above the Int256 maximum (here the UInt256 maximum) is rejected, not silently corrupted.
    node.query("TRUNCATE TABLE test_u256")
    node.query(
        "INSERT INTO test_u256 VALUES "
        "(115792089237316195423570985008687907853269984665640564039457584007913129639935)"
    )
    error = node.query_and_get_error(f"SELECT x FROM {pg_source('default', 'test_u256')}")
    assert "out of range of Int256" in error, error


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


def test_copy_query_initializes_catalog_on_fresh_connection(started_cluster):
    # `COPY (query) TO STDOUT` must create the emulated `pg_catalog` views before it runs the copied query,
    # exactly as an ordinary query does. Previously the COPY path skipped that lazy initialization, so a
    # catalog query run as the very first command on a fresh connection failed with `UNKNOWN_TABLE` even
    # though a plain `SELECT` from the same view worked.
    conn = py_psql.connect(
        host=node.ip_address, port=PG_PORT, user="pguser", password="pgpass", database="default"
    )
    try:
        cur = conn.cursor()
        # The very first command on this connection is a COPY of a catalog query.
        out = io.StringIO()
        cur.copy_expert(
            "COPY (SELECT nspname FROM pg_namespace WHERE nspname = 'pg_catalog') TO STDOUT", out
        )
        assert out.getvalue() == "pg_catalog\n"
    finally:
        conn.close()


def test_copy_honours_format_and_no_op_options(started_cluster):
    # Real PostgreSQL clients append data-formatting options to `COPY`. psycopg2's `copy_to`/`copy_from`
    # always send `WITH DELIMITER AS '\t' NULL AS '\N'`, which are exactly our defaults for the text format,
    # so they are no-ops and must be accepted (an earlier version threw for them, and the exception fell
    # through to the regular-query path, which tore the connection down mid-COPY - psycopg2 then reported a
    # lost connection instead of returning the rows). A `DELIMITER` or `NULL` marker that matches the chosen
    # format's default (a tab for text/TSV, a comma for CSV, `\N` for a NULL in either) is accepted; a
    # non-default value is rejected by test_copy_rejects_unsupported_options below.
    conn = py_psql.connect(
        host=node.ip_address, port=PG_PORT, user="pguser", password="pgpass", database="default"
    )
    try:
        cur = conn.cursor()

        # Text format with the delimiter/null options psycopg2 sends: still TSV, options are no-ops.
        out = io.StringIO()
        cur.copy_expert(
            "COPY (SELECT 1 AS a, 2 AS b) TO STDOUT WITH DELIMITER AS '\t' NULL AS '\\N'", out
        )
        assert out.getvalue() == "1\t2\n"

        # The format is still honoured when default-valued options accompany it, in both the legacy and the
        # modern parenthesized spellings. A comma delimiter is the CSV default and `\N` is the default NULL
        # marker for both text and CSV, so these stay no-ops (an empty NULL marker - PostgreSQL's CSV default -
        # is rejected by test_copy_rejects_unsupported_options because ClickHouse still reads and writes `\N`).
        out = io.StringIO()
        cur.copy_expert(
            "COPY (SELECT 1 AS a, 2 AS b) TO STDOUT WITH DELIMITER AS ',' NULL AS '\\N' CSV", out
        )
        assert out.getvalue() == "1,2\n"

        out = io.StringIO()
        cur.copy_expert(
            "COPY (SELECT 1 AS a, 2 AS b) TO STDOUT WITH (FORMAT csv, DELIMITER ',')", out
        )
        assert out.getvalue() == "1,2\n"

        # The connection stayed usable across all of the above (no mid-COPY teardown).
        cur.execute("SELECT 42")
        assert cur.fetchone() == (42,)
    finally:
        conn.close()


def test_copy_rejects_unsupported_options(started_cluster):
    # Only the format (text/CSV) is honoured; a data-formatting option we cannot faithfully apply must be
    # rejected with a clear error instead of being silently ignored (which would stream output that does not
    # match what the client asked for). The rejection is sent as an ordinary error, so - like a failed
    # ordinary query - the connection survives and can be reused after a rollback.
    conn = py_psql.connect(
        host=node.ip_address, port=PG_PORT, user="pguser", password="pgpass", database="default"
    )
    try:
        # A non-default delimiter would otherwise emit commas (CSV) regardless of the requested ';'.
        with pytest.raises(py_psql.Error, match="non-default DELIMITER"):
            cur = conn.cursor()
            cur.copy_expert("COPY (SELECT 1 AS a, 2 AS b) TO STDOUT WITH (FORMAT csv, DELIMITER ';')", io.StringIO())
        conn.rollback()

        # HEADER is not produced by the text output formats used here.
        with pytest.raises(py_psql.Error, match="HEADER"):
            cur = conn.cursor()
            cur.copy_expert("COPY (SELECT 1 AS a, 2 AS b) TO STDOUT WITH (FORMAT csv, HEADER)", io.StringIO())
        conn.rollback()

        # A non-default NULL marker (PostgreSQL's CSV default is an empty field) would otherwise be silently
        # ignored while ClickHouse still reads and writes `\N` - a protocol mismatch, so it is rejected.
        with pytest.raises(py_psql.Error, match="non-default NULL marker"):
            cur = conn.cursor()
            cur.copy_expert("COPY (SELECT 1 AS a, 2 AS b) TO STDOUT WITH (FORMAT csv, NULL '')", io.StringIO())
        conn.rollback()

        # An option we do not interpret at all is rejected by name rather than dropped.
        with pytest.raises(py_psql.Error, match='"QUOTE" option'):
            cur = conn.cursor()
            cur.copy_expert("COPY (SELECT 1 AS a, 2 AS b) TO STDOUT WITH (FORMAT csv, QUOTE '\"')", io.StringIO())
        conn.rollback()

        # The connection is still usable after the rejections.
        cur = conn.cursor()
        cur.execute("SELECT 42")
        assert cur.fetchone() == (42,)
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


def test_wire_types_for_datetime(started_cluster):
    # A direct PostgreSQL client reading `DateTime` / `DateTime64` columns over the wire must see the
    # PostgreSQL `timestamp` OID (1114) in the `RowDescription`, consistent with the table-name path that
    # advertises them as `timestamp` in `pg_attribute` - not the `varchar` fallback (OID 1043).
    node.query("DROP TABLE IF EXISTS test_wire_datetime SYNC")
    node.query(
        "CREATE TABLE test_wire_datetime (dt DateTime, dt64 DateTime64(3)) "
        "ENGINE = MergeTree ORDER BY dt"
    )
    node.query(
        "INSERT INTO test_wire_datetime VALUES ('2023-01-02 03:04:05', '2023-01-02 03:04:05.123')"
    )

    conn = py_psql.connect(
        host=node.ip_address, port=PG_PORT, user="pguser", password="pgpass", database="default"
    )
    try:
        cur = conn.cursor()
        cur.execute("SELECT dt, dt64 FROM test_wire_datetime")
        # 1114 = timestamp (without time zone).
        assert [c.type_code for c in cur.description] == [1114, 1114]
        # The text value is PostgreSQL's timestamp format, so psycopg2 parses it into a Python datetime.
        row = cur.fetchone()
        assert str(row[0]) == "2023-01-02 03:04:05"
        assert str(row[1]) == "2023-01-02 03:04:05.123000"
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


def test_copy_to_stdout_standard_format_spellings(started_cluster):
    # Real PostgreSQL clients spell the COPY format in several ways: as a bare legacy keyword (`CSV`), after
    # `WITH` (`WITH CSV`), or inside the modern parenthesized option list (`WITH (FORMAT CSV)`, `(FORMAT
    # CSV)`). All of these must select the requested format instead of silently falling back to the default
    # TSV, and `binary` in any of these spellings must hit the binary-COPY rejection.
    conn = py_psql.connect(
        host=node.ip_address, port=PG_PORT, user="pguser", password="pgpass", database="default"
    )
    try:
        cur = conn.cursor()

        for spelling in [
            "CSV",
            "WITH CSV",
            "(FORMAT CSV)",
            "WITH (FORMAT CSV)",
            "WITH (FORMAT csv)",
        ]:
            out = io.StringIO()
            cur.copy_expert(f"COPY (SELECT 1 AS a, 2 AS b) TO STDOUT {spelling}", out)
            assert out.getvalue() == "1,2\n", spelling

        # The legacy text spellings map to TSV.
        for spelling in ["TEXT", "WITH TEXT", "WITH (FORMAT text)"]:
            out = io.StringIO()
            cur.copy_expert(f"COPY (SELECT 1 AS a, 2 AS b) TO STDOUT {spelling}", out)
            assert out.getvalue() == "1\t2\n", spelling

        # A binary COPY is rejected regardless of how it is spelled; the connection survives the error, so
        # it can be reused between the attempts (roll back the aborted command first).
        for spelling in ["BINARY", "WITH BINARY", "WITH (FORMAT BINARY)", "(FORMAT binary)"]:
            with pytest.raises(py_psql.Error, match="binary COPY format is not supported"):
                out = io.BytesIO()
                cur.copy_expert(f"COPY (SELECT 1 AS a) TO STDOUT {spelling}", out)
            conn.rollback()
    finally:
        conn.close()


def test_self_connect_skips_materialized_and_alias_columns(started_cluster):
    # The data path streams a table with `SELECT * FROM <table>`, which omits MATERIALIZED / ALIAS / EPHEMERAL
    # columns. The emulated `pg_attribute` must advertise exactly that column set, so the catalog schema and
    # the COPY payload stay aligned; otherwise a client infers more columns than the stream carries and row
    # decoding goes out of sync.
    node.query("DROP TABLE IF EXISTS test_mat_alias SYNC")
    node.query(
        "CREATE TABLE test_mat_alias "
        "(id UInt32, name String, mat UInt32 MATERIALIZED id * 10, al UInt32 ALIAS id + 1, "
        "ep UInt8 EPHEMERAL 5, reg Int32) "
        "ENGINE = MergeTree ORDER BY id"
    )
    node.query("INSERT INTO test_mat_alias (id, name, reg) VALUES (1, 'a', 100), (2, 'b', 200)")

    # The postgresql() reader discovers only the physical columns (id, name, reg) and reads them correctly.
    # If the catalog still advertised the MATERIALIZED/ALIAS columns, the inferred structure would have five
    # columns and this result would not match.
    assert (
        node.query(f"SELECT * FROM {pg_source('default', 'test_mat_alias')} ORDER BY id")
        == "1\ta\t100\n2\tb\t200\n"
    )

    # A raw PostgreSQL client sees the same alignment: the emulated catalog reports three columns for the
    # table, and a bare-table `COPY ... TO STDOUT` streams exactly three fields per row.
    conn = py_psql.connect(
        host=node.ip_address, port=PG_PORT, user="pguser", password="pgpass", database="default"
    )
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT count() FROM pg_attribute WHERE attrelid = "
            "(SELECT oid FROM pg_class WHERE relname = 'test_mat_alias' "
            "AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')) "
            "AND NOT attisdropped AND attnum > 0"
        )
        assert cur.fetchone()[0] == 3

        out = io.StringIO()
        cur.copy_expert("COPY test_mat_alias TO STDOUT", out)
        assert out.getvalue() == "1\ta\t100\n2\tb\t200\n"
    finally:
        conn.close()


def test_explicit_schema_resolves_correct_database(started_cluster):
    # Two databases that both contain a table with the same name must each resolve to the right table through
    # the schema-qualified self-connect lookup. Namespace (database) OIDs are assigned from a dense, unique
    # mapping, so two database names can never share a namespace OID and make the `pg_class` lookup return the
    # wrong row or more than one row for a common table name.
    node.query("DROP DATABASE IF EXISTS pg_ns_a SYNC")
    node.query("DROP DATABASE IF EXISTS pg_ns_b SYNC")
    node.query("CREATE DATABASE pg_ns_a")
    node.query("CREATE DATABASE pg_ns_b")
    node.query("CREATE TABLE pg_ns_a.events (id UInt32, tag String) ENGINE = MergeTree ORDER BY id")
    node.query("CREATE TABLE pg_ns_b.events (id UInt32, tag String) ENGINE = MergeTree ORDER BY id")
    node.query("INSERT INTO pg_ns_a.events VALUES (1, 'a')")
    node.query("INSERT INTO pg_ns_b.events VALUES (2, 'b')")

    assert (
        node.query(
            f"SELECT id, tag FROM postgresql('127.0.0.1:{PG_PORT}', 'default', 'events', 'pguser', 'pgpass', 'pg_ns_a')"
        )
        == "1\ta\n"
    )
    assert (
        node.query(
            f"SELECT id, tag FROM postgresql('127.0.0.1:{PG_PORT}', 'default', 'events', 'pguser', 'pgpass', 'pg_ns_b')"
        )
        == "2\tb\n"
    )
