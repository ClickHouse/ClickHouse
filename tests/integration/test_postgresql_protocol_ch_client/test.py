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

    # No schema qualifier: the table is resolved in the schema the server resolves unqualified names in,
    # which is the connected ('default') database.
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
        # The RowDescription of a direct SELECT advertises the array OID of the element type, so the
        # client decodes the streamed PostgreSQL array literal into a native array.
        assert [d.type_code for d in cur.description] == [1005, 1009]
        assert cur.fetchall() == [([1, 2], ["x", "y"])]
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
        # modern parenthesized spellings. A comma delimiter is the CSV default, and an explicit `NULL '\N'`
        # for CSV is honoured as that marker (see test_copy_csv_null_semantics); with non-null values the
        # output is the same either way.
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


def test_copy_escape_string_option_values(started_cluster):
    # PostgreSQL's escape-string syntax is how clients spell control-character option values without
    # embedding the raw bytes: `DELIMITER E'\t'` and `NULL E'\N'` (a doubled backslash in the SQL text)
    # decode to exactly the tab and `\N` defaults, so they must be accepted as the no-ops their
    # plain-string spellings are, and a non-default value in the same syntax must still be rejected.
    conn = py_psql.connect(
        host=node.ip_address, port=PG_PORT, user="pguser", password="pgpass", database="default"
    )
    try:
        cur = conn.cursor()

        # The text format's defaults in the escape-string spelling: both options are no-ops.
        out = io.StringIO()
        cur.copy_expert(
            "COPY (SELECT 1 AS a, 2 AS b) TO STDOUT WITH DELIMITER E'\\t' NULL E'\\\\N'", out
        )
        assert out.getvalue() == "1\t2\n"

        # The same spelling works in the parenthesized grammar, and an escape-string `NULL E'\N'` for
        # CSV selects the `\N` marker exactly like its plain-string spelling does.
        out = io.StringIO()
        cur.copy_expert(
            "COPY (SELECT 1 AS a, CAST(NULL AS Nullable(Int32)) AS b) TO STDOUT WITH (FORMAT csv, DELIMITER E',', NULL E'\\\\N')",
            out,
        )
        assert out.getvalue() == "1,\\N\n"

        # A non-default value in the escape-string spelling is still rejected, not silently ignored.
        with pytest.raises(py_psql.Error, match="non-default DELIMITER"):
            cur.copy_expert("COPY (SELECT 1 AS a, 2 AS b) TO STDOUT WITH DELIMITER E';'", io.StringIO())
        conn.rollback()

        # The connection stayed usable across all of the above.
        cur.execute("SELECT 42")
        assert cur.fetchone() == (42,)
    finally:
        conn.close()


def test_copy_null_marker_spellings(started_cluster):
    # The handler reports `standard_conforming_strings = on`, so a backslash inside a plain string literal
    # is an ordinary byte: `NULL '\N'` is the supported default marker, while `NULL '\\N'` (a doubled
    # backslash in the SQL text) requests a three-byte marker and must be rejected as non-default rather
    # than silently served as `\N`. The escape-string `E'\\N'` decodes to `\N` and stays accepted.
    conn = py_psql.connect(
        host=node.ip_address, port=PG_PORT, user="pguser", password="pgpass", database="default"
    )
    try:
        cur = conn.cursor()

        # SQL text `NULL AS '\N'` - the default marker, accepted as a no-op for the text format.
        out = io.StringIO()
        cur.copy_expert(
            "COPY (SELECT 1 AS a, CAST(NULL AS Nullable(Int32)) AS b) TO STDOUT WITH NULL AS '\\N'", out
        )
        assert out.getvalue() == "1\t\\N\n"

        # SQL text `NULL AS E'\\N'` - decodes to `\N`, accepted the same way.
        out = io.StringIO()
        cur.copy_expert(
            "COPY (SELECT 1 AS a, CAST(NULL AS Nullable(Int32)) AS b) TO STDOUT WITH NULL AS E'\\\\N'", out
        )
        assert out.getvalue() == "1\t\\N\n"

        # SQL text `NULL AS '\\N'` - a distinct three-byte marker, rejected instead of being mis-served.
        with pytest.raises(py_psql.Error, match="non-default NULL marker"):
            cur.copy_expert(
                "COPY (SELECT 1 AS a, 2 AS b) TO STDOUT WITH NULL AS '\\\\N'", io.StringIO()
            )
        conn.rollback()

        # Same in the parenthesized grammar and for CSV.
        with pytest.raises(py_psql.Error, match="non-default NULL marker"):
            cur.copy_expert(
                "COPY (SELECT 1 AS a, 2 AS b) TO STDOUT WITH (FORMAT csv, NULL '\\\\N')", io.StringIO()
            )
        conn.rollback()

        # The connection stayed usable after the rejections.
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

        # For the text format the only supported NULL marker is the default `\N`; an empty marker would be
        # silently mismatched (ClickHouse's TSV reader/writer keeps `\N`), so it is rejected. (For CSV an
        # empty marker is PostgreSQL's default and is honoured - see test_copy_csv_null_semantics.)
        with pytest.raises(py_psql.Error, match="non-default NULL marker"):
            cur = conn.cursor()
            cur.copy_expert("COPY (SELECT 1 AS a, 2 AS b) TO STDOUT WITH (FORMAT text, NULL '')", io.StringIO())
        conn.rollback()

        # An option we do not interpret at all is rejected by name rather than dropped.
        with pytest.raises(py_psql.Error, match='"QUOTE" option'):
            cur = conn.cursor()
            cur.copy_expert("COPY (SELECT 1 AS a, 2 AS b) TO STDOUT WITH (FORMAT csv, QUOTE '\"')", io.StringIO())
        conn.rollback()

        # A format we do not recognize must be rejected with a clean error too, not throw inside the parser
        # (which would fall through to the regular-query path and tear the connection down instead).
        with pytest.raises(py_psql.Error, match='"JSONEachRow" format'):
            cur = conn.cursor()
            cur.copy_expert("COPY (SELECT 1 AS a, 2 AS b) TO STDOUT WITH FORMAT JSONEachRow", io.StringIO())
        conn.rollback()

        # A missing option value and a stray literal must not silently select the default delimiter or
        # null marker. Both forms are malformed PostgreSQL COPY syntax and stay in-band errors.
        for copy_query in [
            "COPY (SELECT 1 AS a) TO STDOUT WITH (DELIMITER)",
            "COPY (SELECT 1 AS a) TO STDOUT WITH (NULL)",
            "COPY (SELECT 1 AS a) TO STDOUT WITH (FORMAT csv, 'unexpected')",
            "COPY (SELECT 1 AS a) TO STDOUT WITH (DELIMITER csv)",
            "COPY (SELECT 1 AS a) TO STDOUT WITH (NULL on)",
        ]:
            with pytest.raises(py_psql.Error, match="missing or unexpected value"):
                cur = conn.cursor()
                cur.copy_expert(copy_query, io.StringIO())
            conn.rollback()

        # The connection is still usable after the rejections.
        cur = conn.cursor()
        cur.execute("SELECT 42")
        assert cur.fetchone() == (42,)
    finally:
        conn.close()


def test_simple_query_stops_after_copy_error_and_rejects_custom_format(started_cluster):
    # An in-band COPY error terminates its simple-query packet: later statements must not run. Explicit
    # ClickHouse output formats are likewise rejected before they can place non-PostgreSQL bytes on the
    # wire, and both errors leave the connection usable for the next query.
    node.query("DROP TABLE IF EXISTS test_copy_error_stops_packet SYNC")
    conn = py_psql.connect(
        host=node.ip_address, port=PG_PORT, user="pguser", password="pgpass", database="default"
    )
    try:
        cur = conn.cursor()
        with pytest.raises(py_psql.Error, match="binary COPY format is not supported"):
            cur.execute(
                "COPY (SELECT 1) TO STDOUT WITH FORMAT binary; "
                "CREATE TABLE test_copy_error_stops_packet (x UInt8) ENGINE = Memory"
            )
        conn.rollback()
        assert node.query("EXISTS TABLE test_copy_error_stops_packet") == "0\n"

        with pytest.raises(py_psql.Error, match="does not support custom output formats"):
            cur.execute("SELECT 1 FORMAT JSONEachRow")
        conn.rollback()

        cur.execute("SELECT 42")
        assert cur.fetchone() == (42,)
    finally:
        conn.close()


def test_copy_csv_null_semantics(started_cluster):
    # PostgreSQL's CSV convention is that an empty unquoted field means NULL, while a quoted empty string
    # (`""`) stays an empty string; its text-format convention (and ClickHouse's CSV default) is the `\N`
    # marker. `COPY ... CSV` must follow the PostgreSQL convention by default and honour an explicit
    # `NULL '\N'`, in both directions - otherwise nullable values are serialized or parsed incorrectly.
    node.query("DROP TABLE IF EXISTS test_csv_nulls SYNC")
    node.query(
        "CREATE TABLE test_csv_nulls (id UInt32, n Nullable(Int32), s Nullable(String)) "
        "ENGINE = MergeTree ORDER BY id"
    )
    node.query("INSERT INTO test_csv_nulls VALUES (1, NULL, 'x'), (2, 2, NULL), (3, 3, '')")

    conn = py_psql.connect(
        host=node.ip_address, port=PG_PORT, user="pguser", password="pgpass", database="default"
    )
    try:
        cur = conn.cursor()

        # COPY TO: a NULL becomes an empty unquoted field; a non-null empty string stays quoted.
        out = io.StringIO()
        cur.copy_expert(
            "COPY (SELECT id, n, s FROM test_csv_nulls ORDER BY id) TO STDOUT WITH (FORMAT csv)", out
        )
        assert out.getvalue() == '1,,"x"\n2,2,\n3,3,""\n'

        # An explicit `NULL '\N'` selects ClickHouse's/text-format marker instead.
        out = io.StringIO()
        cur.copy_expert(
            "COPY (SELECT id, n, s FROM test_csv_nulls ORDER BY id) TO STDOUT WITH (FORMAT csv, NULL '\\N')",
            out,
        )
        assert out.getvalue() == '1,\\N,"x"\n2,2,\\N\n3,3,""\n'

        # COPY FROM: an empty unquoted field is read back as NULL, a quoted empty string as ''.
        cur.copy_expert(
            "COPY test_csv_nulls FROM STDIN WITH (FORMAT csv)", io.StringIO('4,,"y"\n5,5,\n6,6,""\n')
        )
    finally:
        conn.close()

    assert node.query(
        "SELECT id, n, s, isNull(n), isNull(s) FROM test_csv_nulls WHERE id > 3 ORDER BY id "
        "FORMAT TSV"
    ) == "4\t\\N\ty\t1\t0\n5\t5\t\\N\t0\t1\n6\t6\t\t0\t0\n"


def test_datetime_scale_roundtrip(started_cluster):
    # Every `DateTime` / `DateTime64` column stays on the text fallback and is read back as `String`
    # (`Array(String)` for arrays) with the exact text rendering preserved. PostgreSQL's `timestamp
    # without time zone` cannot carry the time zone the wall-clock text is rendered in: even a column
    # without an explicit zone renders its text in the *source* server's default time zone, and a reader
    # that reconstructed a `DateTime64(p)` would reinterpret that text in its *own* default zone,
    # silently shifting every epoch whenever the zones differ. Text is lossless.
    node.query("DROP TABLE IF EXISTS test_dt_scales SYNC")
    node.query(
        "CREATE TABLE test_dt_scales "
        "(id UInt32, dt DateTime, dt3 DateTime64(3), dt6 DateTime64(6), dt9 DateTime64(9), "
        "adt3 Array(DateTime64(3))) "
        "ENGINE = MergeTree ORDER BY id"
    )
    node.query(
        "INSERT INTO test_dt_scales VALUES "
        "(1, '2023-01-02 03:04:05', '2023-01-02 03:04:05.123', '2023-01-02 03:04:05.123456', "
        "'2023-01-02 03:04:05.123456789', ['2023-01-02 03:04:05.123'])"
    )

    assert node.query(
        "SELECT toTypeName(dt), toTypeName(dt3), toTypeName(dt6), toTypeName(dt9), toTypeName(adt3) "
        f"FROM {pg_source('default', 'test_dt_scales')} LIMIT 1"
    ) == "String\tString\tString\tString\tArray(String)\n"

    assert node.query(
        f"SELECT dt, dt3, dt6, dt9, adt3 FROM {pg_source('default', 'test_dt_scales')} ORDER BY dt"
    ) == (
        "2023-01-02 03:04:05\t2023-01-02 03:04:05.123\t2023-01-02 03:04:05.123456\t"
        "2023-01-02 03:04:05.123456789\t['2023-01-02 03:04:05.123']\n"
    )


def test_datetime64_scale0_stays_text(started_cluster):
    # A `DateTime64(0)` stays on the text fallback like every other `DateTime` flavor, and its values -
    # including those outside the 32-bit `DateTime` 1970..2106 window - are preserved exactly. A top-level
    # `Array(DateTime64(0))` is advertised as a generic text array (`text[]`), so it is read back as
    # `Array(String)` - the array structure is kept and each element carries the full value as text.
    node.query("DROP TABLE IF EXISTS test_dt64_scale0 SYNC")
    node.query(
        "CREATE TABLE test_dt64_scale0 (id UInt32, dt0 DateTime64(0), adt0 Array(DateTime64(0))) "
        "ENGINE = MergeTree ORDER BY id"
    )
    # '2200-01-01 00:00:00' is beyond the 32-bit `DateTime` range, so a lossy timestamp(0) mapping would
    # corrupt it.
    node.query(
        "INSERT INTO test_dt64_scale0 VALUES "
        "(1, '2200-01-01 00:00:00', ['2200-01-01 00:00:00', '1900-01-01 00:00:00'])"
    )

    assert node.query(
        "SELECT toTypeName(dt0), toTypeName(adt0) "
        f"FROM {pg_source('default', 'test_dt64_scale0')} LIMIT 1"
    ) == "String\tArray(String)\n"

    assert node.query(
        f"SELECT dt0, adt0 FROM {pg_source('default', 'test_dt64_scale0')} ORDER BY dt0"
    ) == "2200-01-01 00:00:00\t['2200-01-01 00:00:00','1900-01-01 00:00:00']\n"


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
    # `varchar` OID (1043) in the `RowDescription`, consistent with the table-name path that keeps them
    # on the text fallback in `pg_attribute`: `timestamp without time zone` cannot carry the time zone
    # the text is rendered in, so advertising it would let a reader with a different default time zone
    # silently shift the epochs.
    node.query("DROP TABLE IF EXISTS test_wire_datetime SYNC")
    node.query(
        "CREATE TABLE test_wire_datetime "
        "(dt DateTime, dt64 DateTime64(3), dt64_zero DateTime64(0), dt64_wide DateTime64(9)) "
        "ENGINE = MergeTree ORDER BY dt"
    )
    node.query(
        "INSERT INTO test_wire_datetime VALUES "
        "('2023-01-02 03:04:05', '2023-01-02 03:04:05.123', '2023-01-02 03:04:05', "
        "'2023-01-02 03:04:05.123456789')"
    )

    conn = py_psql.connect(
        host=node.ip_address, port=PG_PORT, user="pguser", password="pgpass", database="default"
    )
    try:
        cur = conn.cursor()
        cur.execute("SELECT dt, dt64, dt64_zero, dt64_wide FROM test_wire_datetime")
        assert [c.type_code for c in cur.description] == [1043, 1043, 1043, 1043]
        # Every value arrives as the exact text rendering in the source server's default time zone.
        row = cur.fetchone()
        assert row[0] == "2023-01-02 03:04:05"
        assert row[1] == "2023-01-02 03:04:05.123"
        assert row[2] == "2023-01-02 03:04:05"
        assert row[3] == "2023-01-02 03:04:05.123456789"
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
            "AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = current_schema())) "
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


def test_current_setting_missing_ok_returns_null(started_cluster):
    # The emulated `current_setting(name, missing_ok)` must distinguish an absent parameter (NULL) from one
    # whose value is the empty string, so it returns `Nullable(String)`: a known parameter yields its value,
    # an unknown parameter with `missing_ok = true` yields NULL (not an empty string), and an unknown
    # parameter without `missing_ok` throws.
    assert node.query("SELECT current_setting('server_version_num')") == "120000\n"

    # An unknown parameter is NULL (rendered as `\N` in TSV), and is genuinely NULL rather than empty.
    assert node.query("SELECT current_setting('no_such_parameter', true)") == "\\N\n"
    assert node.query("SELECT current_setting('no_such_parameter', true) IS NULL") == "1\n"

    # `COALESCE` over the missing_ok form takes the fallback branch, which an empty-string sentinel would not.
    assert (
        node.query("SELECT COALESCE(current_setting('no_such_parameter', true), 'fallback')")
        == "fallback\n"
    )

    # Without `missing_ok`, an unknown parameter throws.
    assert "UNKNOWN_SETTING" in node.query_and_get_error(
        "SELECT current_setting('no_such_parameter')"
    )


def test_datetime_with_timezone_stays_text(started_cluster):
    # PostgreSQL `timestamp without time zone` cannot carry a ClickHouse column's explicit time zone.
    # Advertising e.g. `DateTime('UTC')` as `timestamp` would make the reading side reconstruct a plain
    # `DateTime` and parse the text in the server default time zone, silently shifting the stored epochs
    # whenever the zones differ. Such columns must stay on the text fallback: they are inferred as
    # `String` and the value is the exact text rendering in the column's own time zone (no epoch shift).
    node.query("DROP TABLE IF EXISTS test_dt_tz SYNC")
    node.query(
        "CREATE TABLE test_dt_tz "
        "(id UInt32, dt_utc DateTime('UTC'), dt64_tz DateTime64(3, 'Asia/Istanbul'), dt DateTime) "
        "ENGINE = MergeTree ORDER BY id"
    )
    node.query(
        "INSERT INTO test_dt_tz VALUES "
        "(1, '2024-01-02 03:04:05', '2024-01-02 03:04:05.123', '2024-01-02 03:04:05')"
    )

    # Every `DateTime` flavor falls back to text (String): a column without an explicit zone is no
    # safer than one with it - its text is rendered in the source server's default time zone, which the
    # wire cannot carry either.
    assert node.query(
        "SELECT toTypeName(dt_utc), toTypeName(dt64_tz), toTypeName(dt) "
        f"FROM {pg_source('default', 'test_dt_tz')} LIMIT 1"
    ) == "String\tString\tString\n"

    # The values are the exact rendering in each column's own time zone - nothing is reinterpreted.
    assert node.query(
        f"SELECT dt_utc, dt64_tz, dt FROM {pg_source('default', 'test_dt_tz')} ORDER BY id"
    ) == "2024-01-02 03:04:05\t2024-01-02 03:04:05.123\t2024-01-02 03:04:05\n"

    # The direct wire path agrees: every `DateTime` flavor is varchar (1043).
    conn = py_psql.connect(
        host=node.ip_address, port=PG_PORT, user="pguser", password="pgpass", database="default"
    )
    try:
        cur = conn.cursor()
        cur.execute("SELECT dt_utc, dt64_tz, dt FROM test_dt_tz")
        assert [c.type_code for c in cur.description] == [1043, 1043, 1043]
        row = cur.fetchone()
        assert row[0] == "2024-01-02 03:04:05"
        assert row[1] == "2024-01-02 03:04:05.123"
    finally:
        conn.close()


def test_copy_from_stdin_row_split_across_frames(started_cluster):
    # PostgreSQL `CopyData` frame boundaries are transport-only: a client may split one logical row
    # (or even one field) across several frames, and may pack many rows into one frame. The server must
    # parse the concatenation of all frames as a single stream; a parser restarted per frame would drop
    # the partial trailing row of every frame. psycopg2's `copy_expert` sends one `CopyData` message per
    # `read(size)` call, so a tiny `size` forces splits in the middle of rows and fields.
    node.query("DROP TABLE IF EXISTS test_copy_split SYNC")
    node.query(
        "CREATE TABLE test_copy_split (id UInt32, s String) ENGINE = MergeTree ORDER BY id"
    )

    payload = "".join(f"{i},value-{i}\n" for i in range(1, 101))
    conn = py_psql.connect(
        host=node.ip_address, port=PG_PORT, user="pguser", password="pgpass", database="default"
    )
    try:
        cur = conn.cursor()
        # Each row is about 12 bytes, so size=3 splits every row across several CopyData frames.
        cur.copy_expert(
            "COPY test_copy_split FROM STDIN WITH (FORMAT csv)", io.StringIO(payload), size=3
        )

        # One frame holding many rows (the whole payload in a single CopyData) must work as well.
        cur.copy_expert(
            "COPY test_copy_split FROM STDIN WITH (FORMAT csv)",
            io.StringIO("".join(f"{i},value-{i}\n" for i in range(101, 201))),
            size=1024 * 1024,
        )
    finally:
        conn.close()

    assert node.query("SELECT count(), sum(id) FROM test_copy_split") == "200\t20100\n"
    assert (
        node.query("SELECT s FROM test_copy_split WHERE id IN (42, 142) ORDER BY id")
        == "value-42\nvalue-142\n"
    )


def test_copy_from_stdin_compound_table_name(started_cluster):
    # A schema/database-qualified target such as `COPY db.table FROM STDIN` must resolve to
    # `db`.`table`, not to a single table whose name literally contains a dot. The `COPY ... TO STDOUT`
    # direction already handled compound names, so both directions must agree.
    node.query("DROP DATABASE IF EXISTS copy_ns SYNC")
    node.query("CREATE DATABASE copy_ns")
    node.query(
        "CREATE TABLE copy_ns.dest (id UInt32, s String) ENGINE = MergeTree ORDER BY id"
    )

    conn = py_psql.connect(
        host=node.ip_address, port=PG_PORT, user="pguser", password="pgpass", database="default"
    )
    try:
        cur = conn.cursor()
        cur.copy_expert(
            "COPY copy_ns.dest FROM STDIN WITH (FORMAT csv)",
            io.StringIO("1,one\n2,two\n"),
        )
        # The same compound name must round-trip back out via COPY TO STDOUT.
        out = io.StringIO()
        cur.copy_expert("COPY copy_ns.dest TO STDOUT WITH (FORMAT csv)", out)
    finally:
        conn.close()

    assert node.query("SELECT id, s FROM copy_ns.dest ORDER BY id") == "1\tone\n2\ttwo\n"
    # ClickHouse's CSV output format always quotes `String` values, so the round-tripped rows come back as
    # `1,"one"` / `2,"two"`; the point of this test is that the compound name resolved, not the quoting.
    assert sorted(out.getvalue().splitlines()) == ['1,"one"', '2,"two"']
    node.query("DROP DATABASE copy_ns SYNC")


def test_copy_array_column_round_trips(started_cluster):
    # `COPY ... TO STDOUT` writes arrays in the PostgreSQL literal spelling (`{...}`), because that is what
    # a PostgreSQL client understands. The `COPY ... FROM STDIN` direction must accept the same spelling,
    # otherwise the payload a client just read out cannot be written back.
    node.query("DROP TABLE IF EXISTS copy_arrays SYNC")
    node.query(
        "CREATE TABLE copy_arrays (id UInt32, ints Array(Int32), strs Array(String), "
        "nested Array(Array(Int32)), nulls Array(Nullable(Int32))) ENGINE = MergeTree ORDER BY id"
    )
    node.query(
        "INSERT INTO copy_arrays VALUES "
        "(1, [1, 2, 3], ['a', 'b,c'], [[1, 2], [3, 4]], [1, NULL, 3]), "
        "(2, [], [''], [], [])",
        settings={"async_insert": 0},
    )

    conn = py_psql.connect(
        host=node.ip_address, port=PG_PORT, user="pguser", password="pgpass", database="default"
    )
    try:
        cur = conn.cursor()
        out = io.StringIO()
        cur.copy_expert("COPY copy_arrays TO STDOUT WITH (FORMAT csv)", out)
        payload = out.getvalue()
        # The arrays leave in the PostgreSQL spelling.
        assert "{" in payload and "[" not in payload, payload

        node.query("TRUNCATE TABLE copy_arrays")
        # ... and the very same bytes are accepted back.
        cur.copy_expert(
            "COPY copy_arrays FROM STDIN WITH (FORMAT csv)", io.StringIO(payload)
        )

        # A malformed array literal is reported without touching the table.
        node.query("DROP TABLE IF EXISTS copy_bad_arrays SYNC")
        node.query(
            "CREATE TABLE copy_bad_arrays (ints Array(Int32)) ENGINE = MergeTree ORDER BY tuple()"
        )
        with pytest.raises(py_psql.Error, match="COPY FROM STDIN failed"):
            cur.copy_expert(
                "COPY copy_bad_arrays FROM STDIN WITH (FORMAT csv)",
                io.StringIO('"{1,2"\n'),
            )
        conn.rollback()
        assert node.query("SELECT count() FROM copy_bad_arrays") == "0\n"
        # The connection survives the rejection.
        cur.execute("SELECT 42")
        assert cur.fetchone()[0] == 42
    finally:
        conn.close()

    assert node.query(
        "SELECT id, ints, strs, nested, nulls FROM copy_arrays ORDER BY id"
    ) == (
        "1\t[1,2,3]\t['a','b,c']\t[[1,2],[3,4]]\t[1,NULL,3]\n"
        "2\t[]\t['']\t[]\t[]\n"
    )
    node.query("DROP TABLE copy_arrays SYNC")
    node.query("DROP TABLE copy_bad_arrays SYNC")

    # PostgreSQL multidimensional arrays must be rectangular. Reject a ClickHouse-only ragged value
    # rather than advertising it as a PostgreSQL array literal that another server cannot parse.
    # The failure is observed through `COPY ... TO STDOUT`: a backend-detected error during copy-out
    # is reported with an `ErrorResponse` inside the copy stream and the server reverts to normal
    # processing, as PostgreSQL does, so the client sees the server's message and the connection
    # stays usable.
    node.query("DROP TABLE IF EXISTS copy_ragged SYNC")
    node.query(
        "CREATE TABLE copy_ragged (v Array(Array(Int32))) ENGINE = MergeTree ORDER BY tuple()"
    )
    node.query(
        "INSERT INTO copy_ragged VALUES ([[1, 2], [3]])", settings={"async_insert": 0}
    )
    ragged_conn = py_psql.connect(
        host=node.ip_address, port=PG_PORT, user="pguser", password="pgpass", database="default"
    )
    try:
        ragged_cur = ragged_conn.cursor()
        with pytest.raises(py_psql.Error, match="rectangular"):
            ragged_cur.copy_expert("COPY copy_ragged TO STDOUT", io.StringIO())
        ragged_conn.rollback()
        ragged_cur.execute("SELECT 42")
        assert ragged_cur.fetchone()[0] == 42
    finally:
        ragged_conn.close()
    node.query("DROP TABLE copy_ragged SYNC")


def test_empty_nested_array_round_trips(started_cluster):
    # PostgreSQL prints an empty array as `{}` whatever its dimensionality, so an empty
    # `Array(Array(Int32))` value has no nesting to count on the way back in. It must still read as an
    # empty array instead of being rejected for having fewer dimensions than expected.
    node.query("DROP TABLE IF EXISTS empty_nested SYNC")
    node.query(
        "CREATE TABLE empty_nested (id UInt32, nested Array(Array(Int32))) ENGINE = MergeTree ORDER BY id"
    )
    # The second row is rectangular on purpose: PostgreSQL multidimensional arrays must be
    # rectangular, so a ragged value such as `[[1, 2], []]` is not representable as a PostgreSQL
    # array literal at all (its rejection is covered in `test_copy_array_column_round_trips`).
    node.query(
        "INSERT INTO empty_nested VALUES (1, []), (2, [[1, 2]])",
        settings={"async_insert": 0},
    )

    assert node.query(
        f"SELECT id, nested FROM postgresql('127.0.0.1:{PG_PORT}', 'default', 'empty_nested', "
        f"'pguser', 'pgpass') ORDER BY id"
    ) == "1\t[]\n2\t[[1,2]]\n"

    node.query("DROP TABLE empty_nested SYNC")


class _FailingSource:
    # A file-like object whose read fails partway through, so psycopg2 aborts the COPY FROM by sending
    # a `CopyFail` frontend message instead of `CopyDone`.
    def __init__(self):
        self._sent = False

    def read(self, size=-1):
        if not self._sent:
            self._sent = True
            return "1,one\n"
        raise IOError("simulated local source failure")


def test_copy_from_stdin_client_abort_sends_copy_fail(started_cluster):
    # When a client aborts a `COPY FROM STDIN` (e.g. its local data source errors), libpq sends a
    # `CopyFail` message. The server must treat this as a regular query error - reply with an error and
    # `ReadyForQuery` - so the connection stays usable, rather than as a protocol violation that tears
    # the connection down.
    node.query("DROP TABLE IF EXISTS test_copy_abort SYNC")
    node.query(
        "CREATE TABLE test_copy_abort (id UInt32, s String) ENGINE = MergeTree ORDER BY id"
    )

    conn = py_psql.connect(
        host=node.ip_address, port=PG_PORT, user="pguser", password="pgpass", database="default"
    )
    try:
        cur = conn.cursor()
        with pytest.raises(Exception):
            cur.copy_expert(
                "COPY test_copy_abort FROM STDIN WITH (FORMAT csv)", _FailingSource()
            )
        conn.rollback()

        # The connection survives the aborted copy and can run another statement.
        cur.execute("SELECT 42")
        assert cur.fetchone()[0] == 42
    finally:
        conn.close()


class _FailingSourceAfterManyRows:
    # A file-like object that streams more rows than fit in one insert block (`max_insert_block_size`
    # defaults to ~1M rows) and then fails, so psycopg2 sends a `CopyFail` only after the server has
    # received enough data to have flushed at least one block if it were inserting as frames arrive.
    def __init__(self, chunks=60, rows_per_chunk=25000):
        self._remaining = chunks
        self._chunk = "7\n" * rows_per_chunk

    def read(self, size=-1):
        if self._remaining > 0:
            self._remaining -= 1
            return self._chunk
        raise IOError("simulated local source failure")


def test_copy_from_stdin_abort_leaves_no_partial_rows(started_cluster):
    # Sinks such as `MergeTreeSink` commit parts while an insert streams, so if the `COPY` payload were
    # fed to the insert pipeline as frames arrive, a `CopyFail` after the first flushed block would leave
    # partial rows visible even though the `COPY` reports failure - and a client retry would duplicate
    # them. The payload is staged in full before the insert starts, so an aborted copy must leave the
    # target table empty even when the payload exceeds one insert block (1.5M rows here, above the
    # default `max_insert_block_size`).
    node.query("DROP TABLE IF EXISTS test_copy_abort_multiblock SYNC")
    node.query(
        "CREATE TABLE test_copy_abort_multiblock (id UInt32) ENGINE = MergeTree ORDER BY id"
    )

    conn = py_psql.connect(
        host=node.ip_address, port=PG_PORT, user="pguser", password="pgpass", database="default"
    )
    try:
        cur = conn.cursor()
        with pytest.raises(Exception):
            cur.copy_expert(
                "COPY test_copy_abort_multiblock FROM STDIN WITH (FORMAT csv)",
                _FailingSourceAfterManyRows(),
            )
        conn.rollback()

        # No rows from the aborted copy were committed.
        assert node.query("SELECT count() FROM test_copy_abort_multiblock") == "0\n"

        # The connection survives and a subsequent copy of the same data succeeds without duplicates.
        cur.copy_expert(
            "COPY test_copy_abort_multiblock FROM STDIN WITH (FORMAT csv)",
            io.StringIO("1\n2\n3\n"),
        )
        conn.commit()
    finally:
        conn.close()

    assert node.query("SELECT count() FROM test_copy_abort_multiblock") == "3\n"
    node.query("DROP TABLE test_copy_abort_multiblock SYNC")


class _SourceWithBadRowAtEnd:
    # A file-like object that streams more rows than fit in one insert block and then a row that cannot
    # be parsed into the target column, so the payload is valid until well past the point where the
    # server would have flushed its first block if it inserted while parsing.
    def __init__(self, chunks=60, rows_per_chunk=25000):
        self._remaining = chunks
        self._chunk = "7\n" * rows_per_chunk
        self._tail_sent = False

    def read(self, size=-1):
        if self._remaining > 0:
            self._remaining -= 1
            return self._chunk
        if not self._tail_sent:
            self._tail_sent = True
            return "not_a_number\n"
        return ""


def test_copy_from_stdin_bad_row_leaves_no_partial_rows(started_cluster):
    # The same all-or-nothing guarantee must hold for a server-side parse error, not only for a
    # client-initiated `CopyFail`: the whole staged payload is parsed before anything is pushed to the
    # insert pipeline, because the sink commits parts as the data streams through it and `cancel` is not
    # a rollback. Here the payload exceeds one insert block and the unparsable row comes last, so an
    # implementation that parsed and inserted in one pass would leave the earlier rows visible and
    # duplicate them on the client's retry.
    node.query("DROP TABLE IF EXISTS test_copy_bad_row SYNC")
    node.query(
        "CREATE TABLE test_copy_bad_row (id UInt32) ENGINE = MergeTree ORDER BY id"
    )

    conn = py_psql.connect(
        host=node.ip_address, port=PG_PORT, user="pguser", password="pgpass", database="default"
    )
    try:
        cur = conn.cursor()
        with pytest.raises(py_psql.Error, match="COPY FROM STDIN failed"):
            cur.copy_expert(
                "COPY test_copy_bad_row FROM STDIN WITH (FORMAT csv)",
                _SourceWithBadRowAtEnd(),
            )
        conn.rollback()

        # Nothing from the failed copy was committed.
        assert node.query("SELECT count() FROM test_copy_bad_row") == "0\n"

        # The failure is an ordinary query error, so the connection stays usable and a retry of a valid
        # payload succeeds without duplicates.
        cur.copy_expert(
            "COPY test_copy_bad_row FROM STDIN WITH (FORMAT csv)",
            io.StringIO("1\n2\n3\n"),
        )
        conn.commit()
    finally:
        conn.close()

    assert node.query("SELECT count() FROM test_copy_bad_row") == "3\n"
    node.query("DROP TABLE test_copy_bad_row SYNC")


def test_copy_rejects_unsupported_endpoints(started_cluster):
    # Only the client stream is implemented: `COPY ... TO STDOUT` and `COPY ... FROM STDIN`. Any other copy
    # endpoint - a server-side `PROGRAM`, a file path, or a mismatched `TO STDIN` / `FROM STDOUT` - must be
    # rejected with a clean error rather than served as the client stream, which would make the server drive
    # the wrong side of the protocol (e.g. send a `CopyInResponse` and then block waiting for `CopyData`
    # frames a client running a server-side `PROGRAM` copy never sends). Like the other rejections, this is
    # an ordinary error, so the connection survives and can be reused after a rollback.
    node.query("DROP TABLE IF EXISTS test_copy_endpoint SYNC")
    node.query(
        "CREATE TABLE test_copy_endpoint (id UInt32) ENGINE = MergeTree ORDER BY id"
    )

    conn = py_psql.connect(
        host=node.ip_address, port=PG_PORT, user="pguser", password="pgpass", database="default"
    )
    try:
        with pytest.raises(py_psql.Error, match="a source other than STDIN"):
            cur = conn.cursor()
            cur.copy_expert("COPY test_copy_endpoint FROM PROGRAM 'cat /dev/null'", io.StringIO())
        conn.rollback()

        with pytest.raises(py_psql.Error, match="a destination other than STDOUT"):
            cur = conn.cursor()
            cur.copy_expert("COPY test_copy_endpoint TO '/tmp/does_not_matter.csv'", io.StringIO())
        conn.rollback()

        with pytest.raises(py_psql.Error, match="a source other than STDIN"):
            cur = conn.cursor()
            cur.copy_expert("COPY test_copy_endpoint FROM STDOUT", io.StringIO())
        conn.rollback()

        # The connection is still usable after the rejections.
        cur = conn.cursor()
        cur.execute("SELECT 42")
        assert cur.fetchone()[0] == 42
    finally:
        conn.close()
    node.query("DROP TABLE test_copy_endpoint SYNC")


def test_array_of_datetime64_pre_epoch_roundtrip(started_cluster):
    # `DateTime64` has a valid pre-epoch range, so a negative value inside an array must round-trip
    # through self-connect: the array-element parser must not clamp it to the epoch the way 32-bit
    # `DateTime` parsing does (a regression where `['1969-12-31 23:59:59.500']` came back as
    # `['1970-01-01 00:00:00.000']`).
    node.query("DROP TABLE IF EXISTS test_dt64_pre_epoch SYNC")
    node.query(
        "CREATE TABLE test_dt64_pre_epoch (id UInt32, dt3 DateTime64(3), adt3 Array(DateTime64(3))) "
        "ENGINE = MergeTree ORDER BY id"
    )
    node.query(
        "INSERT INTO test_dt64_pre_epoch VALUES "
        "(1, '1969-12-31 23:59:59.500', ['1969-12-31 23:59:59.500', '2023-01-02 03:04:05.123'])"
    )

    assert node.query(
        f"SELECT dt3, adt3 FROM {pg_source('default', 'test_dt64_pre_epoch')}"
    ) == ("1969-12-31 23:59:59.500\t['1969-12-31 23:59:59.500','2023-01-02 03:04:05.123']\n")
    node.query("DROP TABLE test_dt64_pre_epoch SYNC")


def test_copy_from_stdin_large_payload_spills_to_disk(started_cluster):
    # The `COPY FROM STDIN` payload is staged in full before the insert starts (to keep the commit
    # boundary after `CopyDone`), but only the first megabyte is kept in memory - the rest spills to a
    # temporary file. A payload well above the in-memory staging limit must be ingested completely and
    # correctly through the spill path.
    node.query("DROP TABLE IF EXISTS test_copy_large SYNC")
    node.query(
        "CREATE TABLE test_copy_large (id UInt32, s String) ENGINE = MergeTree ORDER BY id"
    )

    row_count = 200000
    # Each row is ~18 bytes, so the payload is ~3.5 MB - several times the 1 MiB in-memory staging
    # limit, guaranteeing the temporary-file spill is exercised.
    payload = "".join(f"{i},payload-{i:07d}\n" for i in range(1, row_count + 1))
    assert len(payload) > 3 * 1024 * 1024

    conn = py_psql.connect(
        host=node.ip_address, port=PG_PORT, user="pguser", password="pgpass", database="default"
    )
    try:
        cur = conn.cursor()
        cur.copy_expert(
            "COPY test_copy_large FROM STDIN WITH (FORMAT csv)", io.StringIO(payload)
        )
        conn.commit()
    finally:
        conn.close()

    assert node.query("SELECT count(), sum(id), uniqExact(s) FROM test_copy_large") == (
        f"{row_count}\t{row_count * (row_count + 1) // 2}\t{row_count}\n"
    )
    assert (
        node.query("SELECT s FROM test_copy_large WHERE id IN (1, 199999) ORDER BY id")
        == "payload-0000001\npayload-0199999\n"
    )
    node.query("DROP TABLE test_copy_large SYNC")


def test_transaction_control_noops_only_exact_wrappers(started_cluster):
    # Client libraries wrap statements in plain transaction control (`BEGIN`, `COMMIT`, `ROLLBACK`, ...),
    # which the server acknowledges as no-op success. Only the exact wrapper spellings PostgreSQL defines
    # for plain transaction control are safe to acknowledge: savepoint and two-phase-commit variants
    # (`ROLLBACK TO SAVEPOINT s`, `COMMIT PREPARED 'gid'`, ...) are not implemented, and acknowledging
    # them as success would make ORMs that rely on savepoints silently lose their rollback semantics.
    accepted = [
        "BEGIN",
        "BEGIN WORK",
        "BEGIN TRANSACTION",
        "BEGIN READ ONLY",
        "BEGIN ISOLATION LEVEL SERIALIZABLE, READ ONLY",
        "BEGIN ISOLATION LEVEL REPEATABLE READ",
        "BEGIN READ WRITE, NOT DEFERRABLE",
        "START TRANSACTION ISOLATION LEVEL READ COMMITTED",
        "COMMIT",
        "COMMIT WORK",
        "COMMIT AND NO CHAIN",
        "END TRANSACTION",
        "ROLLBACK",
        "ROLLBACK WORK AND CHAIN",
        "ABORT",
    ]
    conn = py_psql.connect(
        host=node.ip_address, port=PG_PORT, user="pguser", password="pgpass", database="default"
    )
    try:
        cur = conn.cursor()
        for statement in accepted:
            cur.execute(statement)
        # The wrappers were all no-ops and the connection is still fully usable.
        cur.execute("SELECT 42")
        assert cur.fetchone()[0] == 42
    finally:
        conn.close()

    rejected = [
        "SAVEPOINT s",
        "ROLLBACK TO SAVEPOINT s",
        "ROLLBACK TO s",
        "ROLLBACK PREPARED 'gid'",
        "COMMIT PREPARED 'gid'",
        "BEGIN UNRECOGNIZED MODE",
        "COMMIT TRANSACTION EXTRA",
    ]
    for statement in rejected:
        # An unsupported command must produce an error, not a false `CommandComplete`. Each statement
        # gets a fresh connection because a regular query error tears the connection down.
        conn = py_psql.connect(
            host=node.ip_address, port=PG_PORT, user="pguser", password="pgpass", database="default"
        )
        try:
            cur = conn.cursor()
            with pytest.raises(Exception):
                cur.execute(statement)
                cur.fetchall()
        finally:
            conn.close()


def test_relation_oids_are_stable_within_a_session(started_cluster):
    # PostgreSQL OIDs are identifiers, not per-query ranks: a client may cache an OID, or resolve it in one
    # catalog query and follow it in another. Creating or dropping an earlier-sorting table must not renumber
    # existing relations within a session, and a table created after the connection was opened must still
    # become visible with a fresh OID.
    node.query("DROP TABLE IF EXISTS aaa_oid_probe SYNC")
    node.query("DROP TABLE IF EXISTS zzz_oid_target SYNC")
    node.query("CREATE TABLE zzz_oid_target (x UInt32) ENGINE = MergeTree ORDER BY x")

    conn = py_psql.connect(
        host=node.ip_address, port=PG_PORT, user="pguser", password="pgpass", database="default"
    )
    try:
        cur = conn.cursor()

        def relation_oid(name):
            cur.execute(f"SELECT oid FROM pg_class WHERE relname = '{name}' ORDER BY oid LIMIT 1")
            rows = cur.fetchall()
            return rows[0][0] if rows else None

        oid_before = relation_oid("zzz_oid_target")
        assert oid_before is not None

        # A new table whose (database, name) sorts before the existing one would have shifted every later
        # row_number rank; a stable mapping must keep the old OID and give the new table a fresh one.
        node.query("CREATE TABLE aaa_oid_probe (x UInt32) ENGINE = MergeTree ORDER BY x")
        assert relation_oid("zzz_oid_target") == oid_before
        new_oid = relation_oid("aaa_oid_probe")
        assert new_oid is not None
        assert new_oid != oid_before

        # Dropping the earlier-sorting table must not renumber the survivor either, and the dropped
        # relation must disappear from the catalog.
        node.query("DROP TABLE aaa_oid_probe SYNC")
        assert relation_oid("zzz_oid_target") == oid_before
        assert relation_oid("aaa_oid_probe") is None
    finally:
        conn.close()
        node.query("DROP TABLE IF EXISTS zzz_oid_target SYNC")


def test_oids_survive_rename(started_cluster):
    # An OID observed once must keep referring to the same object. `RENAME` preserves the UUID (in `Atomic`
    # databases, the default), so the catalog must keep the OID and expose the relation - and the namespace -
    # under the new name, and a client that cached `pg_class.oid` must still be able to follow it through
    # `pg_attribute.attrelid` after the rename.
    node.query("DROP TABLE IF EXISTS rename_oid_before SYNC")
    node.query("DROP TABLE IF EXISTS rename_oid_after SYNC")
    node.query("DROP DATABASE IF EXISTS rename_oid_db SYNC")
    node.query("DROP DATABASE IF EXISTS rename_oid_db2 SYNC")
    node.query("CREATE TABLE rename_oid_before (x UInt32) ENGINE = MergeTree ORDER BY x")
    node.query("CREATE DATABASE rename_oid_db")

    conn = py_psql.connect(
        host=node.ip_address, port=PG_PORT, user="pguser", password="pgpass", database="default"
    )
    try:
        cur = conn.cursor()

        def relation_oid(name):
            cur.execute(f"SELECT oid FROM pg_class WHERE relname = '{name}' ORDER BY oid LIMIT 1")
            rows = cur.fetchall()
            return rows[0][0] if rows else None

        def namespace_oid(name):
            cur.execute(f"SELECT oid FROM pg_namespace WHERE nspname = '{name}'")
            rows = cur.fetchall()
            return rows[0][0] if rows else None

        table_oid = relation_oid("rename_oid_before")
        assert table_oid is not None
        db_oid = namespace_oid("rename_oid_db")
        assert db_oid is not None

        node.query("RENAME TABLE rename_oid_before TO rename_oid_after")
        assert relation_oid("rename_oid_before") is None
        assert relation_oid("rename_oid_after") == table_oid

        # The cached OID still resolves the relation's columns after the rename.
        cur.execute(f"SELECT attname FROM pg_attribute WHERE attrelid = {table_oid}")
        assert [row[0] for row in cur.fetchall()] == ["x"]

        node.query("RENAME DATABASE rename_oid_db TO rename_oid_db2")
        assert namespace_oid("rename_oid_db") is None
        assert namespace_oid("rename_oid_db2") == db_oid
    finally:
        conn.close()
        node.query("DROP TABLE IF EXISTS rename_oid_before SYNC")
        node.query("DROP TABLE IF EXISTS rename_oid_after SYNC")
        node.query("DROP DATABASE IF EXISTS rename_oid_db SYNC")
        node.query("DROP DATABASE IF EXISTS rename_oid_db2 SYNC")


def test_catalog_refresh_trigger_is_case_insensitive(started_cluster):
    # Unquoted PostgreSQL identifiers are case-insensitive, so the catalog refresh gate must not depend on
    # the spelling of `pg_`. OIDs are assigned append-only in observation order, which makes the gate
    # observable without an erroring statement (a query error would tear down the session and its OID
    # state): a table created before an uppercase-`PG_`-only statement must get a smaller OID than a table
    # created after it. If that statement failed to trigger the refresh, both tables would be numbered
    # together by the next lowercase catalog read, ordered by name, and the later-created `aaa_*` table
    # would get the smaller OID.
    node.query("DROP TABLE IF EXISTS zzz_upper_first SYNC")
    node.query("DROP TABLE IF EXISTS aaa_upper_second SYNC")

    conn = py_psql.connect(
        host=node.ip_address, port=PG_PORT, user="pguser", password="pgpass", database="default"
    )
    try:
        cur = conn.cursor()

        def relation_oid(name):
            cur.execute(f"SELECT oid FROM pg_class WHERE relname = '{name}' ORDER BY oid LIMIT 1")
            rows = cur.fetchall()
            return rows[0][0] if rows else None

        # Number every pre-existing relation first, so the two probe tables are the only new ones.
        cur.execute("SELECT count() FROM pg_class")
        cur.fetchall()

        node.query("CREATE TABLE zzz_upper_first (x UInt32) ENGINE = MergeTree ORDER BY x")
        # The only `pg_` occurrence in this statement is uppercase; it must still refresh the catalog OIDs.
        cur.execute("SELECT 1 AS PG_NAMESPACE_PROBE")
        assert cur.fetchall() == [(1,)]
        node.query("CREATE TABLE aaa_upper_second (x UInt32) ENGINE = MergeTree ORDER BY x")

        first_oid = relation_oid("zzz_upper_first")
        second_oid = relation_oid("aaa_upper_second")
        assert first_oid is not None and second_oid is not None
        assert first_oid < second_oid
    finally:
        conn.close()
        node.query("DROP TABLE IF EXISTS zzz_upper_first SYNC")
        node.query("DROP TABLE IF EXISTS aaa_upper_second SYNC")


def test_public_schema_is_only_a_real_database(started_cluster):
    # `public` is not synthesized as an alias of the connected database: a schema name always denotes the
    # ClickHouse database of that name, so schema discovery and the `COPY` that reads the rows can never
    # resolve one unqualified name to two different tables. An unqualified lookup resolves through
    # `current_schema()`, that is in the connected database, even while a real `public` database with a
    # same-named table exists; `schema='public'` reaches that real database, and nothing at all once it is
    # dropped.
    node.query("DROP DATABASE IF EXISTS public SYNC")
    node.query("DROP TABLE IF EXISTS default.pub_probe SYNC")
    node.query("CREATE TABLE default.pub_probe (v UInt32) ENGINE = MergeTree ORDER BY v")
    node.query("INSERT INTO default.pub_probe VALUES (1)")
    try:
        node.query("CREATE DATABASE public")
        node.query("CREATE TABLE public.pub_probe (v UInt32) ENGINE = MergeTree ORDER BY v")
        node.query("INSERT INTO public.pub_probe VALUES (2)")

        # No schema qualifier: both the column discovery and the rows come from the connected database,
        # never from the same-named table of the real `public` database.
        assert node.query(f"SELECT v FROM {pg_source('default', 'pub_probe')}") == "1\n"

        # Schema-qualified `public` resolves to the real database of that name.
        assert (
            node.query(
                f"SELECT v FROM postgresql('127.0.0.1:{PG_PORT}', 'default', 'pub_probe', 'pguser', 'pgpass', 'public')"
            )
            == "2\n"
        )
        # The connected database stays reachable under its own name as well.
        assert (
            node.query(
                f"SELECT v FROM postgresql('127.0.0.1:{PG_PORT}', 'default', 'pub_probe', 'pguser', 'pgpass', 'default')"
            )
            == "1\n"
        )

        node.query("DROP DATABASE public SYNC")

        # An unqualified lookup keeps working, while `schema='public'` now fails cleanly instead of
        # silently falling back to a table of the connected database.
        assert node.query(f"SELECT v FROM {pg_source('default', 'pub_probe')}") == "1\n"
        with pytest.raises(Exception, match="does not exist"):
            node.query(
                f"SELECT v FROM postgresql('127.0.0.1:{PG_PORT}', 'default', 'pub_probe', 'pguser', 'pgpass', 'public')"
            )
    finally:
        node.query("DROP DATABASE IF EXISTS public SYNC")
        node.query("DROP TABLE IF EXISTS default.pub_probe SYNC")


def test_copy_quoted_column_names(started_cluster):
    # A quoted column in a `COPY` column list must stay a single exact identifier when the statement is
    # rewritten into ClickHouse SQL (`INSERT INTO ... (...)` / `SELECT ... FROM ...`): dropping the quoting
    # would turn `"a.b"` into a compound reference and `"select"` into a keyword. pqxx's `stream_to` always
    # emits a quoted column list, so this is the normal self-connect insert path, in both directions.
    node.query("DROP TABLE IF EXISTS test_copy_quoted_cols SYNC")
    node.query(
        "CREATE TABLE test_copy_quoted_cols (`a.b` UInt32, `select` String) ENGINE = MergeTree ORDER BY `a.b`"
    )

    conn = py_psql.connect(
        host=node.ip_address, port=PG_PORT, user="pguser", password="pgpass", database="default"
    )
    try:
        cur = conn.cursor()
        cur.copy_expert(
            'COPY test_copy_quoted_cols ("a.b", "select") FROM STDIN WITH (FORMAT csv)',
            io.StringIO("1,one\n2,two\n"),
        )
        out = io.StringIO()
        cur.copy_expert(
            'COPY test_copy_quoted_cols ("a.b", "select") TO STDOUT WITH (FORMAT csv)', out
        )
    finally:
        conn.close()

    assert (
        node.query("SELECT `a.b`, `select` FROM test_copy_quoted_cols ORDER BY `a.b`")
        == "1\tone\n2\ttwo\n"
    )
    assert sorted(out.getvalue().splitlines()) == ['1,"one"', '2,"two"']
    node.query("DROP TABLE test_copy_quoted_cols SYNC")


def test_catalog_refresh_per_statement(started_cluster):
    # The catalog OID refresh must run against each statement that actually executes, not against the outer
    # simple-query message text: the SQL behind `EXECUTE s` is resolved from the prepared statement, and in a
    # semicolon-separated multi-statement message a table created by an earlier statement must already be
    # visible to a later catalog read within the same message.
    node.query("DROP TABLE IF EXISTS refresh_prepare_probe SYNC")
    node.query("DROP TABLE IF EXISTS refresh_multi_probe SYNC")

    conn = py_psql.connect(
        host=node.ip_address, port=PG_PORT, user="pguser", password="pgpass", database="default"
    )
    try:
        cur = conn.cursor()

        # `PREPARE` the catalog read while the table does not exist yet; the outer `EXECUTE` text contains
        # no `pg_`, so only a refresh against the resolved prepared SQL makes the new table visible.
        cur.execute(
            "PREPARE refresh_probe_stmt AS "
            "SELECT relname FROM pg_class WHERE relname = 'refresh_prepare_probe'"
        )
        node.query(
            "CREATE TABLE refresh_prepare_probe (x UInt32) ENGINE = MergeTree ORDER BY x"
        )
        cur.execute("EXECUTE refresh_probe_stmt")
        rows = cur.fetchall()
        assert rows and all(row[0] == "refresh_prepare_probe" for row in rows)

        # DDL followed by a catalog read in one simple-query message: the refresh has to happen per split
        # statement, after the `CREATE TABLE` ran, not once up front where the table does not exist yet.
        cur.execute(
            "CREATE TABLE refresh_multi_probe (x UInt32) ENGINE = MergeTree ORDER BY x; "
            "SELECT relname FROM pg_class WHERE relname = 'refresh_multi_probe'"
        )
        rows = cur.fetchall()
        assert rows and all(row[0] == "refresh_multi_probe" for row in rows)
    finally:
        conn.close()
        node.query("DROP TABLE IF EXISTS refresh_prepare_probe SYNC")
        node.query("DROP TABLE IF EXISTS refresh_multi_probe SYNC")


def test_pg_class_oid_is_unique_per_row(started_cluster):
    # A table is exposed exactly once in `pg_class`, under the namespace of the database that owns it:
    # clients treat `pg_class.oid` as the unique relation identifier, so a second row for the same table
    # would duplicate every column in joins that follow it (e.g. `pg_attribute.attrelid = pg_class.oid`).
    node.query("DROP TABLE IF EXISTS oid_unique_probe SYNC")
    node.query(
        "CREATE TABLE oid_unique_probe (a UInt32, b String) ENGINE = MergeTree ORDER BY a"
    )

    conn = py_psql.connect(
        host=node.ip_address, port=PG_PORT, user="pguser", password="pgpass", database="default"
    )
    try:
        cur = conn.cursor()
        cur.execute("SELECT oid FROM pg_class WHERE relname = 'oid_unique_probe'")
        oids = [row[0] for row in cur.fetchall()]
        assert len(oids) == 1, oids

        # Following the OID through the `attrelid` join must yield each column exactly once.
        for oid in oids:
            cur.execute(
                "SELECT a.attname FROM pg_class c "
                "JOIN pg_attribute a ON a.attrelid = c.oid "
                f"WHERE c.oid = {oid} ORDER BY a.attnum"
            )
            assert [row[0] for row in cur.fetchall()] == ["a", "b"]
    finally:
        conn.close()
        node.query("DROP TABLE IF EXISTS oid_unique_probe SYNC")


def assert_statement_rejected(statement):
    # A statement that reaches normal query processing and fails there is answered with an
    # `ErrorResponse` and then tears the connection down: the handler rethrows after replying and the
    # run loop ends. So each rejection is checked on a connection of its own - reusing the caller's
    # connection would leave every later assertion running into a socket this rejection already closed.
    conn = py_psql.connect(
        host=node.ip_address, port=PG_PORT, user="pguser", password="pgpass", database="default"
    )
    try:
        with pytest.raises(Exception):
            conn.cursor().execute(statement)
    finally:
        conn.close()


def test_jdbc_set_noop_requires_exact_statement(started_cluster):
    # The JDBC handshake's `SET extra_float_digits` / `SET application_name` are acknowledged as no-ops
    # only when the packet is exactly such a single statement. A query merely containing that text as a
    # literal must be executed, not swallowed with a fake `CommandComplete`.
    conn = py_psql.connect(
        host=node.ip_address, port=PG_PORT, user="pguser", password="pgpass", database="default"
    )
    try:
        cur = conn.cursor()

        # The handshake forms stay accepted as no-ops.
        cur.execute("SET extra_float_digits = 3")
        assert cur.statusmessage == "SET", cur.statusmessage
        cur.execute("SET application_name = 'PostgreSQL JDBC Driver'")
        assert cur.statusmessage == "SET", cur.statusmessage
        cur.execute("SET application_name TO 'probe'")
        assert cur.statusmessage == "SET", cur.statusmessage

        # The value of `application_name` comes from a user-supplied connection string, so it may
        # contain semicolons. Such a statement is still exactly one statement and must be accepted:
        # the single-statement check has to skip quoted literals instead of splitting on the first `;`.
        cur.execute("SET application_name TO 'jdbc;a'")
        assert cur.statusmessage == "SET", cur.statusmessage
        cur.execute("SET application_name = 'a;b;c'")
        assert cur.statusmessage == "SET", cur.statusmessage
        cur.execute("SET application_name TO 'quo''ted;value'")
        assert cur.statusmessage == "SET", cur.statusmessage
        cur.execute("SET application_name TO 'trailing;'")
        assert cur.statusmessage == "SET", cur.statusmessage
        cur.execute("SET application_name TO 'jdbc;a';")
        assert cur.statusmessage == "SET", cur.statusmessage

        # The value is arbitrarily long (drivers put free-form client info there): the classifier must
        # scan the whole statement, not a fixed-size prefix that a long value would overflow, pushing a
        # perfectly valid no-op into real `SET` processing.
        cur.execute("SET application_name TO '%s'" % ("x" * 500))
        assert cur.statusmessage == "SET", cur.statusmessage
        cur.execute("SET application_name = '%s'" % ("long;value " * 50))
        assert cur.statusmessage == "SET", cur.statusmessage
        # A trailing statement is still rejected even when it is pushed far out by a long value.
        assert_statement_rejected("SET application_name TO '%s'; SELECT 1" % ("x" * 500))

        # A non-ASCII value is not a reason to reject the handshake either: the classifier only folds
        # ASCII, so bytes above 0x7F pass through instead of being handed to the locale-dependent ctype
        # functions (undefined behavior for a negative `char`).
        cur.execute("SET application_name TO 'Приложение'")
        assert cur.statusmessage == "SET", cur.statusmessage

        # A query containing the magic text as a literal is executed, not swallowed.
        cur.execute("SELECT 'SET application_name'")
        assert cur.fetchall() == [("SET application_name",)]

        # A literal that merely looks like a second statement does not make the query a no-op either.
        cur.execute("SELECT 'SET application_name TO ''x''; SELECT 1'")
        assert cur.fetchall() == [("SET application_name TO 'x'; SELECT 1",)]

    finally:
        conn.close()

    # A multi-statement packet is not acknowledged by the fast path: it falls through to normal
    # processing, where the unsupported `SET` fails loudly instead of silently dropping the trailing
    # statement.
    assert_statement_rejected("SET application_name TO 'x'; SELECT 1")


def test_copy_to_stdout_zero_rows(started_cluster):
    # A `COPY ... TO STDOUT` whose result has no rows at all must complete cleanly ("COPY 0") and leave
    # the connection usable. The per-row serialization finalizes its write buffer after each row, so a
    # result that never enters the row loop used to leave the buffer neither finalized nor canceled -
    # a logical error (an exception in a sanitizer build) that took the whole server down.
    node.query("DROP TABLE IF EXISTS test_copy_zero_rows SYNC")
    node.query(
        "CREATE TABLE test_copy_zero_rows (id UInt32, s String) ENGINE = MergeTree ORDER BY id"
    )
    try:
        conn = py_psql.connect(
            host=node.ip_address, port=PG_PORT, user="pguser", password="pgpass", database="default"
        )
        try:
            cur = conn.cursor()
            # An empty table and an empty subquery result both take the zero-row path.
            out = io.StringIO()
            cur.copy_expert("COPY test_copy_zero_rows TO STDOUT", out)
            assert out.getvalue() == ""
            out = io.StringIO()
            cur.copy_expert(
                "COPY (SELECT number FROM system.numbers WHERE number < 0 LIMIT 1) TO STDOUT",
                out,
            )
            assert out.getvalue() == ""
            # The connection survives and keeps working.
            cur.execute("SELECT 1")
            assert cur.fetchall() == [(1,)]
        finally:
            conn.close()

        # The self-connect read path issues the same zero-row `COPY` for an empty table (and for the
        # catalog probe of a table that does not exist), and must come back empty rather than kill the
        # server.
        assert (
            node.query(f"SELECT count() FROM {pg_source('default', 'test_copy_zero_rows')}")
            == "0\n"
        )
        # The server is still alive.
        assert node.query("SELECT 1") == "1\n"
    finally:
        node.query("DROP TABLE IF EXISTS test_copy_zero_rows SYNC")


def test_search_path_reports_connected_database(started_cluster):
    # `current_setting('search_path')` must report the database unqualified names actually resolve in -
    # the connected database (`current_schema()`) - not PostgreSQL's default `public`. A client that
    # discovers the default schema through this function must arrive where the server itself resolves
    # unqualified names.
    node.query("DROP DATABASE IF EXISTS spath_db SYNC")
    node.query("CREATE DATABASE spath_db")
    try:
        for database in ["default", "spath_db"]:
            conn = py_psql.connect(
                host=node.ip_address,
                port=PG_PORT,
                user="pguser",
                password="pgpass",
                database=database,
            )
            try:
                cur = conn.cursor()
                cur.execute("SELECT current_setting('search_path')")
                assert cur.fetchall() == [(database,)]
                # It agrees with `current_schema()`, the other discovery mechanism.
                cur.execute("SELECT current_schema()")
                assert cur.fetchall() == [(database,)]
            finally:
                conn.close()
    finally:
        node.query("DROP DATABASE IF EXISTS spath_db SYNC")
