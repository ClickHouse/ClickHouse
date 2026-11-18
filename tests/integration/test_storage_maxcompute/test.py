import json
import re
import tempfile
from contextlib import contextmanager
from pathlib import Path

import pytest
import requests

from helpers.cluster import ClickHouseCluster

from .fixtures import generate_fixtures


NORMAL_ENDPOINT = "http://maxcompute1:8080"
STRICT_ENDPOINT = "http://maxcompute-strict:8080"
TYPES_ENDPOINT = "http://maxcompute-types:8080"

SCALAR_COLUMNS = (
    "id Int64, name Nullable(String), score Float64, enabled Nullable(UInt8)"
)
SEQ_COLUMNS = "id Int64, payload String, bucket Int64"
COMPLEX_NATIVE_COLUMNS = (
    "id Int64, arr Array(Nullable(Int64)), "
    "attrs Nested(key Nullable(String), value Nullable(Int64)), "
    "obj Tuple(Nullable(Int64), Nullable(String))"
)
COMPLEX_STRING_COLUMNS = "id Int64, arr String, attrs String, obj String"

SCALAR_ORACLE = """
SELECT count() = 8
    AND sum(id) = 12
    AND countIf(isNull(name)) = 1
    AND countIf(name = '') = 1
    AND countIf(name = '中文') = 1
    AND countIf(name = 'quote-s') = 1
    AND countIf(name = 'tab\\\\tvalue') = 1
    AND countIf(name = 'line\\\\nvalue') = 1
    AND countIf(isNull(enabled)) = 2
    AND sum(ifNull(enabled, 0)) = 3
FROM {table}
"""
SEQ_ORACLE = (
    "SELECT count(), uniqExact(id), min(id), max(id), sum(id), sum(bucket) "
    "FROM {table}"
)
SEQ_EXPECTED = "10007\t10007\t0\t10006\t50065021\t80023\n"


_fixture_directory = tempfile.TemporaryDirectory(prefix="clickhouse-maxcompute-")
_fixture_path = Path(_fixture_directory.name) / "fixtures.sql"
generate_fixtures(_fixture_path)

cluster = ClickHouseCluster(__file__)
cluster.maxcompute_fixtures_path = str(_fixture_path)
cluster.maxcompute_auth_path = str(Path(__file__).parent / "auth.json")
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml"],
    users_configs=["configs/experimental_maxcompute.xml"],
    with_maxcompute=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/remote_servers.xml"],
    users_configs=["configs/experimental_maxcompute.xml"],
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        cluster.wait_for_url(
            f"http://127.0.0.1:{cluster.maxcompute_port}/readyz", timeout=60
        )
        yield cluster
    finally:
        cluster.shutdown()
        _fixture_directory.cleanup()


def table_name(case_id):
    return "mc_" + re.sub("[^a-z0-9]+", "_", case_id.lower()).strip("_")


def maxcompute_ddl(
    name,
    remote_table,
    columns,
    *,
    engine="MaxComputeRaw",
    endpoint=NORMAL_ENDPOINT,
    project="test_project",
    partition="",
    access_key_id="test-ak",
    access_key_secret="test-sk",
    threads=1,
    quota="default",
    start=None,
    count=None,
    table_format=None,
):
    args = [
        repr(endpoint),
        repr(project),
        repr(remote_table),
        repr(partition),
        repr(access_key_id),
        repr(access_key_secret),
        str(threads),
        repr(quota),
    ]
    if start is not None:
        args.extend([str(start), str(count)])
    ddl = f"CREATE TABLE {name} ({columns}) ENGINE = {engine}({', '.join(args)})"
    if table_format is not None:
        ddl += f" SETTINGS maxcompute_read_format = {table_format!r}"
    return ddl


def legacy_maxcompute_ddl(name, remote_table, columns, start, count):
    args = [
        repr(NORMAL_ENDPOINT),
        repr("test_project"),
        repr(remote_table),
        repr(""),
        repr("test-ak"),
        repr("test-sk"),
        "1",
        str(start),
        str(count),
    ]
    return f"CREATE TABLE {name} ({columns}) ENGINE = MaxComputeRaw({', '.join(args)})"


@contextmanager
def source_table(case_id, remote_table, columns, **kwargs):
    name = table_name(case_id)
    node1.query(f"DROP TABLE IF EXISTS {name} SYNC")
    node1.query(maxcompute_ddl(name, remote_table, columns, **kwargs))
    try:
        yield name
    finally:
        node1.query(f"DROP TABLE IF EXISTS {name} SYNC")


def assert_error_contains(error, expected):
    assert expected.lower() in error.lower(), error


@pytest.mark.parametrize(
    "engine,settings",
    [
        pytest.param(
            "MaxComputeRaw",
            {"maxcompute_read_format": "row", "odps_read_compress": 0},
            id="MC-E2E-010",
        ),
        pytest.param(
            "MaxComputeRaw",
            {"maxcompute_read_format": "row", "odps_read_compress": 1},
            id="MC-E2E-011",
        ),
        pytest.param(
            "MaxCompute",
            {"maxcompute_read_format": "row", "odps_read_compress": 0},
            id="MC-E2E-012",
        ),
        pytest.param(
            "MaxCompute",
            {"maxcompute_read_format": "row", "odps_read_compress": 1},
            id="MC-E2E-013",
        ),
    ],
)
def test_scalar_row_compatibility(request, engine, settings):
    with source_table(
        request.node.callspec.id,
        "e2e_scalar",
        SCALAR_COLUMNS,
        engine=engine,
    ) as table:
        assert node1.query(SCALAR_ORACLE.format(table=table), settings=settings) == "1\n"


def test_projection_reads_only_requested_columns():
    with source_table("MC-E2E-014", "e2e_scalar", SCALAR_COLUMNS) as table:
        assert (
            node1.query(
                f"SELECT count() = 8 AND countIf(isNull(name)) = 1 "
                f"FROM (SELECT id, name FROM {table})",
                settings={"maxcompute_read_format": "row", "odps_read_compress": 0},
            )
            == "1\n"
        )
        assert (
            node1.query(
                f"SELECT count() = 8 AND countIf(isNull(enabled)) = 2 "
                f"FROM (SELECT enabled FROM {table})",
                settings={"maxcompute_read_format": "row", "odps_read_compress": 0},
            )
            == "1\n"
        )


def test_empty_table():
    with source_table(
        "MC-E2E-015", "e2e_empty", "id Int64, value Nullable(String)"
    ) as table:
        assert (
            node1.query(
                f"SELECT count() FROM {table}",
                settings={"maxcompute_read_format": "row", "odps_read_compress": 0},
            )
            == "0\n"
        )


def test_default_and_empty_quota():
    for quota in ("default", ""):
        with source_table(
            "MC-E2E-016",
            "e2e_scalar",
            SCALAR_COLUMNS,
            quota=quota,
        ) as table:
            assert (
                node1.query(
                    f"SELECT count() FROM {table}",
                    settings={
                        "maxcompute_read_format": "row",
                        "odps_read_compress": 0,
                    },
                )
                == "8\n"
            )


@pytest.mark.parametrize(
    "settings",
    [
        pytest.param({"odps_read_compress": 0}, id="default"),
        pytest.param(
            {"maxcompute_read_format": "inherit", "odps_read_compress": 0},
            id="inherit",
        ),
        pytest.param(
            {"maxcompute_read_format": "row", "odps_read_compress": 0},
            id="MC-E2E-017c",
        ),
        pytest.param(
            {"maxcompute_read_format": "column", "odps_read_compress": 0},
            id="MC-E2E-017d",
        ),
        pytest.param(
            {"maxcompute_read_format": "column", "odps_read_compress": 0},
            id="MC-E2E-020",
        ),
        pytest.param(
            {
                "maxcompute_read_format": "column",
                "odps_read_compress": 1,
                "maxcompute_max_retries": 0,
            },
            id="MC-E2E-021a",
        ),
    ],
)
def test_scalar_format_selection(request, settings):
    with source_table(
        request.node.callspec.id, "e2e_scalar", SCALAR_COLUMNS
    ) as table:
        assert node1.query(SCALAR_ORACLE.format(table=table), settings=settings) == "1\n"


@pytest.mark.parametrize("engine", ["MaxCompute", "MaxComputeRaw"])
@pytest.mark.parametrize("table_format", [None, "row", "column"])
@pytest.mark.parametrize("query_format", [None, "inherit", "row", "column"])
def test_table_read_format_precedence(request, engine, table_format, query_format):
    with source_table(
        request.node.name,
        "e2e_complex",
        COMPLEX_STRING_COLUMNS,
        engine=engine,
        table_format=table_format,
    ) as table:
        settings = {"odps_read_compress": 0}
        if query_format is not None:
            settings["maxcompute_read_format"] = query_format
        effective_format = (
            query_format
            if query_format not in (None, "inherit")
            else table_format or "column"
        )
        query = f"SELECT count() = 3 AND sum(length(arr)) > 0 FROM {table}"
        if effective_format == "column":
            # Unrequested complex columns must not prevent reading scalar columns with Arrow.
            assert node1.query(
                f"SELECT count(), sum(id) FROM {table}", settings=settings
            ) == "3\t6\n"
            assert_error_contains(
                node1.query_and_get_error(query, settings=settings),
                "cannot be read with the Arrow format",
            )
        else:
            assert node1.query(query, settings=settings) == "1\n"


@pytest.mark.parametrize("engine", ["MaxCompute", "MaxComputeRaw"])
def test_table_read_format_alter_and_reattach(request, engine):
    with source_table(
        request.node.name,
        "e2e_complex",
        COMPLEX_STRING_COLUMNS,
        engine=engine,
        table_format="row",
    ) as table:
        query = f"SELECT count() = 3 AND sum(length(arr)) > 0 FROM {table}"
        settings = {"odps_read_compress": 0}
        assert node1.query(query, settings=settings) == "1\n"
        node1.query(f"ALTER TABLE {table} MODIFY SETTING maxcompute_read_format = 'column'")
        assert "maxcompute_read_format = 'column'" in node1.query(f"SHOW CREATE TABLE {table}")
        node1.query(f"DETACH TABLE {table} SYNC")
        node1.query(f"ATTACH TABLE {table}")
        assert_error_contains(
            node1.query_and_get_error(query, settings=settings),
            "cannot be read with the Arrow format",
        )
        assert_error_contains(
            node1.query_and_get_error(
                f"ALTER TABLE {table} MODIFY SETTING maxcompute_read_format = 'bogus'"
            ),
            "Unknown `maxcompute_read_format`",
        )
        assert "maxcompute_read_format = 'column'" in node1.query(f"SHOW CREATE TABLE {table}")
        node1.query(f"ALTER TABLE {table} MODIFY SETTING maxcompute_read_format = 'row'")
        assert node1.query(query, settings=settings) == "1\n"
        node1.query(f"ALTER TABLE {table} RESET SETTING maxcompute_read_format")
        assert "maxcompute_read_format" not in node1.query(f"SHOW CREATE TABLE {table}")
        node1.query(f"DETACH TABLE {table} SYNC")
        node1.query(f"ATTACH TABLE {table}")
        assert_error_contains(
            node1.query_and_get_error(query, settings=settings),
            "cannot be read with the Arrow format",
        )


@pytest.mark.parametrize("engine", ["MaxCompute", "MaxComputeRaw"])
@pytest.mark.parametrize("table_format", [None, "row", "column"])
def test_reset_read_format_restores_column_reader(request, engine, table_format):
    with source_table(
        request.node.name,
        "e2e_scalar",
        SCALAR_COLUMNS,
        engine=engine,
        table_format=table_format,
    ) as table:
        settings = {"maxcompute_read_format": "inherit", "odps_read_compress": 0}

        def assert_column_read(stage):
            assert "SETTINGS" not in node1.query(f"SHOW CREATE TABLE {table}")
            query_id = f"{table}_{stage}"
            assert node1.query(
                f"SELECT count(), sum(id) FROM {table}",
                settings=settings,
                query_id=query_id,
            ) == "8\t12\n"
            node1.wait_for_log_line(
                re.escape("{" + query_id + "}") + r".*MaxCompute Arrow reader finished\."
            )
            assert not node1.contains_in_log(
                f"{{{query_id}}}.*MaxCompute reader finished."
            )

        node1.query(f"ALTER TABLE {table} RESET SETTING maxcompute_read_format")
        assert_column_read("reset")
        node1.query(f"ALTER TABLE {table} RESET SETTING maxcompute_read_format")
        assert_column_read("repeat_reset")
        node1.query(f"DETACH TABLE {table} SYNC")
        node1.query(f"ATTACH TABLE {table}")
        assert_column_read("reattach")
        node1.query(f"ALTER TABLE {table} RESET SETTING maxcompute_read_format")
        assert_column_read("reset_after_reattach")

        node1.query(f"ALTER TABLE {table} MODIFY SETTING maxcompute_read_format = 'row'")
        query_id = f"{table}_modify_again"
        assert node1.query(
            f"SELECT count(), sum(id) FROM {table}", settings=settings, query_id=query_id
        ) == "8\t12\n"
        node1.wait_for_log_line(
            re.escape("{" + query_id + "}") + r".*MaxCompute reader finished\."
        )
        node1.query(f"ALTER TABLE {table} RESET SETTING maxcompute_read_format")
        assert_column_read("reset_again")


def test_session_read_format_and_query_override():
    with source_table(
        "format-session", "e2e_complex", COMPLEX_STRING_COLUMNS, table_format="row"
    ) as table:
        params = {"session_id": table}
        query = f"SELECT count() = 3 AND sum(length(arr)) > 0 FROM {table}"
        try:
            node1.http_query("SET maxcompute_read_format = 'column'", params=params)
            assert_error_contains(
                node1.http_query_and_get_error(query, params=params),
                "cannot be read with the Arrow format",
            )
            assert node1.http_query(
                query + " SETTINGS maxcompute_read_format = 'row'", params=params
            ) == "1\n"
            assert node1.http_query(
                query + " SETTINGS maxcompute_read_format = 'inherit'", params=params
            ) == "1\n"
            # A query override must not change the session's setting.
            assert node1.http_query("SELECT getSetting('maxcompute_read_format')", params=params) == "column\n"
            node1.http_query("SET maxcompute_read_format = 'inherit'", params=params)
            assert node1.http_query(query, params=params) == "1\n"
        finally:
            node1.http_query("SET maxcompute_read_format = 'inherit'", params=params)


def test_independent_table_read_formats_in_one_query():
    with source_table(
        "format-multiple-row", "e2e_complex", COMPLEX_STRING_COLUMNS, table_format="row"
    ) as row_table, source_table(
        "format-multiple-column", "e2e_scalar", SCALAR_COLUMNS, table_format="column"
    ) as column_table:
        query = (
            f"SELECT (SELECT sum(length(arr)) > 0 FROM {row_table}) "
            f"AND (SELECT count() = 8 FROM {column_table})"
        )
        assert node1.query(query, settings={"odps_read_compress": 0}) == "1\n"
        assert_error_contains(
            node1.query_and_get_error(query, settings={"maxcompute_read_format": "column"}),
            "cannot be read with the Arrow format",
        )


def test_forced_arrow_rejects_stringified_complex_types():
    with source_table(
        "MC-E2E-017e", "e2e_complex", COMPLEX_STRING_COLUMNS
    ) as table:
        error = node1.query_and_get_error(
            f"SELECT sum(length(arr)) FROM {table}",
            settings={"maxcompute_read_format": "column", "odps_read_compress": 0},
        )
        assert_error_contains(error, "cannot be read with the Arrow format")




@pytest.mark.parametrize(
    "settings,expected",
    [
        pytest.param(
            {"maxcompute_read_format": "bogus", "odps_read_compress": 0},
            "Unknown `maxcompute_read_format`",
            id="MC-E2E-017h",
        ),
    ],
)
def test_invalid_format_settings(request, settings, expected):
    with source_table(
        request.node.callspec.id, "e2e_scalar", SCALAR_COLUMNS
    ) as table:
        assert_error_contains(
            node1.query_and_get_error(f"SELECT count() FROM {table}", settings=settings),
            expected,
        )


@pytest.mark.parametrize(
    "batch_bytes,query,expected",
    [
        pytest.param(0, SEQ_ORACLE, SEQ_EXPECTED, id="MC-E2E-022"),
        pytest.param(128, SEQ_ORACLE, SEQ_EXPECTED, id="MC-E2E-023"),
        pytest.param(
            128,
            "SELECT count() = 3 AND sum(length(payload)) = 27 AND sum(id) = 15009 "
            "FROM (SELECT payload, id FROM {table} WHERE id IN (0, 5003, 10006))",
            "1\n",
            id="MC-E2E-024",
        ),
    ],
)
def test_arrow_multibatch_and_projection(request, batch_bytes, query, expected):
    with source_table(
        request.node.callspec.id, "e2e_seq_10007", SEQ_COLUMNS
    ) as table:
        assert (
            node1.query(
                query.format(table=table),
                settings={
                    "maxcompute_read_format": "column",
                    "odps_read_compress": 0,
                    "maxcompute_columnar_max_batch_bytes": batch_bytes,
                },
            )
            == expected
        )


def test_arrow_native_complex_types():
    with source_table(
        "MC-E2E-025", "e2e_complex", COMPLEX_NATIVE_COLUMNS
    ) as table:
        assert (
            node1.query(
                f"SELECT count() = 3 AND countIf(id = 2 AND arr = [1] "
                f"AND attrs.key = ['a'] AND attrs.value = [10] "
                f"AND obj = (20, 'two')) = 1 FROM {table}",
                settings={"maxcompute_read_format": "column", "odps_read_compress": 0},
            )
            == "1\n"
        )


def test_row_complex_string_conversion():
    with source_table(
        "MC-E2E-026", "e2e_complex", COMPLEX_STRING_COLUMNS
    ) as table:
        assert (
            node1.query(
                f"SELECT count() = 3 AND countIf(id = 2 AND length(arr) > 0 "
                f"AND length(attrs) > 0 AND length(obj) > 0) = 1 FROM {table}",
                settings={"maxcompute_read_format": "row", "odps_read_compress": 0},
            )
            == "1\n"
        )


@pytest.mark.parametrize(
    "read_format,expected",
    [
        pytest.param("row", "Cannot cast MaxCompute data type ODPS_TIMESTAMP", id="MC-E2E-027a"),
        pytest.param("column", "cannot be read with the Arrow format", id="MC-E2E-027b"),
    ],
)
def test_timestamp_string_conversion_is_rejected(request, read_format, expected):
    with source_table(
        request.node.callspec.id,
        "e2e_temporal_decimal",
        "id Int64, ts String",
    ) as table:
        assert_error_contains(
            node1.query_and_get_error(
                f"SELECT sum(length(ts)) FROM {table}",
                settings={"maxcompute_read_format": read_format, "odps_read_compress": 0},
            ),
            expected,
        )


@pytest.mark.parametrize("read_format", ["row", "column"])
@pytest.mark.parametrize("null_as_default", [0, 1])
def test_non_nullable_null_is_rejected(request, read_format, null_as_default):
    with source_table(request.node.name, "e2e_scalar", "name String") as table:
        assert_error_contains(
            node1.query_and_get_error(
                f"SELECT name FROM {table}",
                settings={
                    "maxcompute_read_format": read_format,
                    "input_format_null_as_default": null_as_default,
                    "odps_read_compress": 0,
                },
            ),
            "NULL",
        )


@pytest.mark.parametrize("read_format", ["row", "column"])
def test_temporal_nulls_remain_null(request, read_format):
    with source_table(
        request.node.name,
        "e2e_temporal_nulls",
        "id Int64, d Nullable(Date), dt Nullable(DateTime('UTC'))",
    ) as table:
        assert node1.query(
            f"SELECT count(), countIf(isNull(d)), countIf(isNull(dt)) FROM {table}",
            settings={"maxcompute_read_format": read_format, "input_format_null_as_default": 1},
        ) == "2\t1\t1\n"


@pytest.mark.parametrize("read_format", ["row", "column"])
def test_temporal_bounds_and_subseconds(request, read_format):
    with source_table(
        request.node.name,
        "e2e_temporal_bounds",
        "id Int64, d Date, dt DateTime('UTC')",
    ) as table:
        assert node1.query(
            f"SELECT id, toUInt16(d), toUInt32(dt) FROM {table} ORDER BY id",
            settings={"maxcompute_read_format": read_format, "odps_read_compress": 0},
        ) == "0\t0\t0\n1\t1\t0\n2\t65535\t4294967295\n"


@pytest.mark.parametrize("read_format", ["row", "column"])
@pytest.mark.parametrize("overflow_behavior", ["ignore", "saturate", "throw"])
@pytest.mark.parametrize("nullable", [False, True])
@pytest.mark.parametrize(
    "remote_table,column_type",
    [
        ("e2e_date_before_epoch", "Date"),
        ("e2e_date_after_max", "Date"),
        ("e2e_datetime_before_epoch", "DateTime('UTC')"),
        ("e2e_datetime_after_max", "DateTime('UTC')"),
    ],
)
def test_temporal_overflow_is_rejected(
    request, read_format, overflow_behavior, nullable, remote_table, column_type
):
    if nullable:
        column_type = f"Nullable({column_type})"
    with source_table(request.node.name, remote_table, f"v {column_type}") as table:
        assert_error_contains(
            node1.query_and_get_error(
                f"SELECT v FROM {table}",
                settings={
                    "maxcompute_read_format": read_format,
                    "date_time_overflow_behavior": overflow_behavior,
                    "odps_read_compress": 0,
                },
            ),
            "CANNOT_CONVERT_TYPE",
        )


@pytest.mark.parametrize("read_format", ["row", "column"])
@pytest.mark.parametrize(
    "columns",
    [
        "dates Array(Nullable(Date))",
        "attrs Nested(key Nullable(String), value Nullable(Date))",
        "obj Tuple(Nullable(DateTime('UTC')))",
    ],
)
def test_nested_temporal_overflow_is_rejected(request, read_format, columns):
    with source_table(request.node.name, "e2e_temporal_nested", columns) as table:
        assert_error_contains(
            node1.query_and_get_error(
                f"SELECT * FROM {table}",
                settings={
                    "maxcompute_read_format": read_format,
                    "date_time_overflow_behavior": "saturate",
                    "odps_read_compress": 0,
                },
            ),
            "CANNOT_CONVERT_TYPE",
        )


@pytest.mark.parametrize(
    "threads,start,count,read_format,settings,expected",
    [
        pytest.param(1, 0, 0, "row", {}, SEQ_EXPECTED, id="MC-E2E-030"),
        pytest.param(2, 0, 0, "row", {}, SEQ_EXPECTED, id="MC-E2E-031a"),
        pytest.param(3, 0, 0, "row", {}, SEQ_EXPECTED, id="MC-E2E-031b"),
        pytest.param(16, 0, 0, "row", {}, SEQ_EXPECTED, id="MC-E2E-031c"),
        pytest.param(3, 100, 1003, "row", {}, "1003\t1003\t100\t1102\t602803\n", id="MC-E2E-032"),
        pytest.param(3, 100, 1003, "column", {}, "1003\t1003\t100\t1102\t602803\n", id="MC-E2E-037"),
        pytest.param(
            16,
            0,
            0,
            "row",
            {"odps_parallel_local_insert_select": 0},
            SEQ_EXPECTED,
            id="MC-E2E-038a",
        ),
        pytest.param(
            16,
            0,
            0,
            "row",
            {"odps_parallel_local_insert_select": 1},
            SEQ_EXPECTED,
            id="MC-E2E-038b",
        ),
    ],
)
def test_parallel_ranges(
    request, threads, start, count, read_format, settings, expected
):
    with source_table(
        request.node.callspec.id,
        "e2e_seq_10007",
        SEQ_COLUMNS,
        threads=threads,
        start=start,
        count=count,
    ) as table:
        query = SEQ_ORACLE
        if count:
            query = (
                "SELECT count(), uniqExact(id), min(id), max(id), sum(id) FROM {table}"
            )
        query_settings = {
            "maxcompute_read_format": read_format,
            "odps_read_compress": 0,
            **settings,
        }
        assert node1.query(query.format(table=table), settings=query_settings) == expected
        assert (
            node1.query(
                f"SELECT count() FROM (SELECT id, count() AS c FROM {table} "
                f"GROUP BY id HAVING c != 1)",
                settings=query_settings,
            )
            == "0\n"
        )


@pytest.mark.parametrize(
    "start,count,threads,expected",
    [
        pytest.param(10000, 100, 3, "[10000,10001,10002,10003,10004,10005,10006]\n", id="MC-E2E-033"),
        pytest.param(10007, 0, 1, "[]\n", id="MC-E2E-034"),
        pytest.param(0, 2, 16, "[0,1]\n", id="MC-E2E-036"),
    ],
)
def test_range_boundaries(request, start, count, threads, expected):
    with source_table(
        request.node.callspec.id,
        "e2e_seq_10007",
        SEQ_COLUMNS,
        threads=threads,
        start=start,
        count=count,
    ) as table:
        assert (
            node1.query(
                f"SELECT groupArray(id) FROM (SELECT id FROM {table} ORDER BY id)",
                settings={"maxcompute_read_format": "row", "odps_read_compress": 0},
            )
            == expected
        )


def test_range_start_after_end_is_rejected():
    with source_table(
        "MC-E2E-035",
        "e2e_seq_10007",
        SEQ_COLUMNS,
        start=10008,
        count=0,
    ) as table:
        assert_error_contains(
            node1.query_and_get_error(
                f"SELECT count() FROM {table}",
                settings={"maxcompute_read_format": "row", "odps_read_compress": 0},
            ),
            "larger than total record count",
        )


def test_download_session_lifecycle():
    before = cluster.get_container_logs("maxcompute1")
    with source_table(
        "MC-E2E-039",
        "e2e_seq_10007",
        SEQ_COLUMNS,
        threads=3,
        start=100,
        count=1003,
    ) as table:
        assert (
            node1.query(
                SEQ_ORACLE.format(table=table),
                settings={"maxcompute_read_format": "row", "odps_read_compress": 0},
            )
            == "1003\t1003\t100\t1102\t602803\n"
        )

    delta = cluster.get_container_logs("maxcompute1")[len(before) :]
    events = []
    for line in delta.splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if event.get("project") == "test_project" and event.get("table") == "e2e_seq_10007":
            events.append(event)

    actions = [event.get("action") for event in events]
    assert actions.count("create") == 1, events
    assert actions.count("complete") == 1, events
    assert actions.count("read") >= 1, events
    hashes = {event.get("download_id_hash") for event in events if event.get("download_id_hash")}
    assert len(hashes) == 1, events


@pytest.mark.parametrize(
    "partition,expected",
    [
        pytest.param("p_date=2026-09-13", "[0,1,2,3,4]\n", id="MC-E2E-040"),
        pytest.param("p_date=2026-09-14", "[5,6,7,8,9,10,11]\n", id="MC-E2E-041"),
    ],
)
def test_partition_reads(request, partition, expected):
    with source_table(
        request.node.callspec.id,
        "e2e_partitioned",
        "id Int64, value String",
        partition=partition,
    ) as table:
        assert (
            node1.query(
                f"SELECT groupArray(id) FROM (SELECT id FROM {table} ORDER BY id)",
                settings={"maxcompute_read_format": "row", "odps_read_compress": 0},
            )
            == expected
        )


def test_missing_partition_is_distinct_from_an_empty_result():
    with source_table(
        "MC-E2E-042",
        "e2e_partitioned",
        "id Int64, value String",
        partition="p_date=2099-01-01",
    ) as table:
        assert_error_contains(
            node1.query_and_get_error(
                f"SELECT count() FROM {table}",
                settings={"maxcompute_read_format": "row", "odps_read_compress": 0},
            ),
            "NoSuchPartition",
        )


def test_nullable_scalar_values():
    with source_table("MC-E2E-043", "e2e_scalar", SCALAR_COLUMNS) as table:
        assert (
            node1.query(
                SCALAR_ORACLE.format(table=table),
                settings={"maxcompute_read_format": "row", "odps_read_compress": 0},
            )
            == "1\n"
        )


def test_temporal_and_decimal_values():
    columns = (
        "id Int64, amount32 Decimal32(2), amount64 Decimal64(4), "
        "amount128 Decimal128(9), d Date, dt DateTime('UTC')"
    )
    with source_table(
        "MC-E2E-044", "e2e_temporal_decimal", columns
    ) as table:
        assert (
            node1.query(
                f"SELECT count() = 2 AND countIf(id = 2 AND amount32 = -12.34 "
                f"AND amount64 = 123456789.1234 "
                f"AND amount128 = 12345678901234567890123456789.123456789 "
                f"AND d = toDate('2026-09-14') "
                f"AND dt = toDateTime('2026-09-14 12:34:56', 'UTC')) = 1 "
                f"FROM {table}",
                settings={"maxcompute_read_format": "row", "odps_read_compress": 0},
            )
            == "1\n"
        )


def test_complex_types_match_between_row_and_arrow():
    with source_table(
        "MC-E2E-045", "e2e_complex", COMPLEX_NATIVE_COLUMNS
    ) as table:
        for read_format in ("row", "column"):
            assert (
                node1.query(
                    f"SELECT count() = 3 FROM {table}",
                    settings={
                        "maxcompute_read_format": read_format,
                        "odps_read_compress": 0,
                    },
                )
                == "1\n"
            )


@pytest.mark.parametrize(
    "columns",
    [
        pytest.param("id Int64, arr Array(Int64)", id="MC-E2E-046a"),
        pytest.param("id Int64, obj Tuple(Int64, String)", id="MC-E2E-046b"),
        pytest.param("id Int64, obj Tuple(Nullable(Int64), String)", id="MC-E2E-046c"),
    ],
)
def test_unsupported_complex_schema_is_rejected(request, columns):
    name = table_name(request.node.callspec.id)
    node1.query(f"DROP TABLE IF EXISTS {name} SYNC")
    try:
        assert_error_contains(
            node1.query_and_get_error(maxcompute_ddl(name, "e2e_complex", columns)),
            "Unsupported column type",
        )
    finally:
        node1.query(f"DROP TABLE IF EXISTS {name} SYNC")


def test_legacy_and_quota_ddl_argument_layouts_match():
    legacy = table_name("MC-E2E-047-legacy")
    current = table_name("MC-E2E-047-current")
    for table in (legacy, current):
        node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
    try:
        node1.query(legacy_maxcompute_ddl(legacy, "e2e_seq_10007", SEQ_COLUMNS, 100, 10))
        node1.query(
            maxcompute_ddl(
                current,
                "e2e_seq_10007",
                SEQ_COLUMNS,
                start=100,
                count=10,
            )
        )
        for table in (legacy, current):
            assert (
                node1.query(
                    f"SELECT groupArray(id) FROM (SELECT id FROM {table} ORDER BY id)",
                    settings={"maxcompute_read_format": "row", "odps_read_compress": 0},
                )
                == "[100,101,102,103,104,105,106,107,108,109]\n"
            )
    finally:
        for table in (legacy, current):
            node1.query(f"DROP TABLE IF EXISTS {table} SYNC")


@pytest.mark.parametrize(
    "read_format,start,count,transform,expected",
    [
        pytest.param("row", 0, 0, False, SEQ_EXPECTED, id="MC-E2E-050"),
        pytest.param("column", 0, 0, False, SEQ_EXPECTED, id="MC-E2E-051"),
        pytest.param("row", 100, 1003, False, "1003\t1003\t100\t1102\t602803\n", id="MC-E2E-052"),
        pytest.param(
            "row",
            0,
            0,
            True,
            "[(0,'ROW-00000',0),(5003,'ROW-05003',10),(10006,'ROW-10006',20)]\n",
            id="MC-E2E-053",
        ),
    ],
)
def test_insert_select(request, read_format, start, count, transform, expected):
    case_id = request.node.callspec.id
    source = table_name(case_id + "-source")
    target = table_name(case_id + "-target")
    node1.query(f"DROP TABLE IF EXISTS {source} SYNC")
    node1.query(f"DROP TABLE IF EXISTS {target} SYNC")
    try:
        node1.query(
            maxcompute_ddl(
                source,
                "e2e_seq_10007",
                SEQ_COLUMNS,
                threads=3 if count else 1,
                start=start,
                count=count,
            )
        )
        if transform:
            node1.query(
                f"CREATE TABLE {target} "
                f"(id Int64, payload_upper String, bucket2 Int64) "
                f"ENGINE = MergeTree ORDER BY id"
            )
            node1.query(
                f"INSERT INTO {target} SELECT id, upper(payload), bucket * 2 "
                f"FROM {source} WHERE id IN (0, 5003, 10006)",
                settings={"maxcompute_read_format": read_format, "odps_read_compress": 0},
            )
            assert (
                node1.query(
                    f"SELECT groupArray((id, payload_upper, bucket2)) "
                    f"FROM (SELECT * FROM {target} ORDER BY id)"
                )
                == expected
            )
        else:
            node1.query(
                f"CREATE TABLE {target} ({SEQ_COLUMNS}) ENGINE = MergeTree ORDER BY id"
            )
            node1.query(
                f"INSERT INTO {target} SELECT * FROM {source}",
                settings={"maxcompute_read_format": read_format, "odps_read_compress": 0},
            )
            query = SEQ_ORACLE
            if count:
                query = "SELECT count(), uniqExact(id), min(id), max(id), sum(id) FROM {table}"
            assert node1.query(query.format(table=target)) == expected
    finally:
        node1.query(f"DROP TABLE IF EXISTS {source} SYNC")
        node1.query(f"DROP TABLE IF EXISTS {target} SYNC")


def create_distributed_tables(case_id, *, source_start=0, source_count=0):
    suffix = table_name(case_id)
    source = suffix + "_source"
    local = suffix + "_local"
    distributed = suffix + "_distributed"
    for node in (node1, node2):
        node.query(f"DROP TABLE IF EXISTS {local} SYNC")
        node.query(f"CREATE TABLE {local} ({SEQ_COLUMNS}) ENGINE = MergeTree ORDER BY id")
    node1.query(f"DROP TABLE IF EXISTS {source} SYNC")
    node1.query(f"DROP TABLE IF EXISTS {distributed} SYNC")
    node1.query(
        maxcompute_ddl(
            source,
            "e2e_seq_10007",
            SEQ_COLUMNS,
            threads=5,
            start=source_start,
            count=source_count,
        )
    )
    return source, local, distributed


def drop_distributed_tables(source, local, distributed):
    node1.query(f"DROP TABLE IF EXISTS {distributed} SYNC")
    node1.query(f"DROP TABLE IF EXISTS {source} SYNC")
    for node in (node1, node2):
        node.query(f"DROP TABLE IF EXISTS {local} SYNC")


def distributed_oracle(local, include_bucket=True):
    aggregates = "count(), uniqExact(id), min(id), max(id), sum(id)"
    if include_bucket:
        aggregates += ", sum(bucket)"
    return (
        f"SELECT {aggregates} FROM clusterAllReplicas"
        f"('mc_e2e_cluster', default, {local})"
    )


def test_distributed_without_sharding_key_is_rejected():
    source, local, distributed = create_distributed_tables("MC-E2E-060")
    try:
        node1.query(
            f"CREATE TABLE {distributed} AS {local} "
            f"ENGINE = Distributed(mc_e2e_cluster, default, {local})"
        )
        error = node1.query_and_get_error(
            f"INSERT INTO {distributed} SELECT * FROM {source}",
            settings={
                "distributed_foreground_insert": 1,
                "maxcompute_read_format": "row",
                "odps_read_compress": 0,
                "enable_insert_from_odps_exteranl_table": 1,
                "odps_parallel_distributed_insert_select": 1,
                "odps_distributed_insert_select_convert_to_local": 1,
            },
        )
        assert_error_contains(error, "STORAGE_REQUIRES_PARAMETER")
        assert node1.query(distributed_oracle(local)) == "0\t0\t0\t0\t0\t0\n"
    finally:
        drop_distributed_tables(source, local, distributed)


@pytest.mark.parametrize(
    "start,count,read_format,compatibility_settings,expected",
    [
        pytest.param(0, 0, "row", {}, SEQ_EXPECTED, id="MC-E2E-061"),
        pytest.param(100, 1003, "row", {}, "1003\t1003\t100\t1102\t602803\n", id="MC-E2E-063"),
        pytest.param(10006, 1, "row", {}, "1\t1\t10006\t10006\t10006\n", id="MC-E2E-064"),
        pytest.param(
            0,
            0,
            "row",
            {
                "enable_insert_from_odps_exteranl_table": 0,
                "enable_insert_from_odps_external_table": 0,
                "odps_parallel_distributed_insert_select": 0,
                "odps_distributed_insert_select_convert_to_local": 0,
            },
            SEQ_EXPECTED,
            id="MC-E2E-065-off",
        ),
        pytest.param(
            0,
            0,
            "row",
            {
                "enable_insert_from_odps_exteranl_table": 1,
                "enable_insert_from_odps_external_table": 1,
                "odps_parallel_distributed_insert_select": 1,
                "odps_distributed_insert_select_convert_to_local": 1,
            },
            SEQ_EXPECTED,
            id="MC-E2E-065-on",
        ),
        pytest.param(0, 0, "column", {}, SEQ_EXPECTED, id="MC-E2E-066"),
    ],
)
def test_standard_distributed_insert(
    request, start, count, read_format, compatibility_settings, expected
):
    source, local, distributed = create_distributed_tables(
        request.node.callspec.id, source_start=start, source_count=count
    )
    try:
        node1.query(
            f"CREATE TABLE {distributed} AS {local} ENGINE = "
            f"Distributed(mc_e2e_cluster, default, {local}, sipHash64(id))"
        )
        node1.query(
            f"INSERT INTO {distributed} SELECT * FROM {source}",
            settings={
                "distributed_foreground_insert": 1,
                "maxcompute_read_format": read_format,
                "odps_read_compress": 0,
                **compatibility_settings,
            },
        )
        assert node1.query(distributed_oracle(local, count == 0)) == expected
    finally:
        drop_distributed_tables(source, local, distributed)


def test_distributed_worker_does_not_open_maxcompute_source():
    source, local, distributed = create_distributed_tables("MC-E2E-062")
    try:
        node1.query(
            f"CREATE TABLE {distributed} AS {local} ENGINE = "
            f"Distributed(mc_e2e_cluster, default, {local}, sipHash64(id))"
        )
        node1.query(
            f"INSERT INTO {distributed} SELECT * FROM {source}",
            settings={
                "distributed_foreground_insert": 1,
                "maxcompute_read_format": "row",
                "odps_read_compress": 0,
            },
            query_id="MC-E2E-062",
        )
        assert node1.query(distributed_oracle(local)) == SEQ_EXPECTED
        worker_log = node2.grep_in_log("MC-E2E-062")
        assert "StorageMaxCompute" not in worker_log
        assert not node1.contains_in_log("NoSuchDownload")
        assert not node2.contains_in_log("NoSuchDownload")
    finally:
        drop_distributed_tables(source, local, distributed)


@pytest.mark.parametrize(
    "endpoint,project,remote_table,expected",
    [
        pytest.param(
            "http://mc-no-such-host:8080",
            "test_project",
            "e2e_scalar",
            "mc-no-such-host",
            id="MC-E2E-070",
        ),
        pytest.param(
            NORMAL_ENDPOINT,
            "test_project",
            "no_such_table",
            "NoSuchTable",
            id="MC-E2E-071",
        ),
        pytest.param(
            NORMAL_ENDPOINT,
            "no_such_project",
            "e2e_scalar",
            "NoSuchProject",
            id="MC-E2E-071b",
        ),
    ],
)
def test_connection_and_metadata_errors(
    request, endpoint, project, remote_table, expected
):
    with source_table(
        request.node.callspec.id,
        remote_table,
        "id Int64",
        endpoint=endpoint,
        project=project,
    ) as table:
        assert_error_contains(
            node1.query_and_get_error(
                f"SELECT count() FROM {table}",
                settings={
                    "maxcompute_read_format": "row",
                    "odps_read_compress": 0,
                    "maxcompute_connect_timeout_ms": 1000,
                    "maxcompute_request_timeout_ms": 1000,
                },
            ),
            expected,
        )


def test_source_access_right_is_required_and_sufficient():
    user = table_name("MC-E2E-080-user")
    table = table_name("MC-E2E-080-table")
    node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node1.query(f"DROP USER IF EXISTS {user}")
    try:
        node1.query(f"CREATE USER {user} IDENTIFIED WITH no_password")
        node1.query(f"GRANT CREATE TABLE, DROP TABLE, SELECT ON default.* TO {user}")
        ddl = maxcompute_ddl(table, "e2e_scalar", SCALAR_COLUMNS)
        assert_error_contains(
            node1.query_and_get_error(ddl, user=user),
            "grant TABLE ENGINE ON MaxComputeRaw",
        )
        node1.query(f"GRANT READ, WRITE ON MAXCOMPUTE TO {user}")
        node1.query(ddl, user=user)
        assert (
            node1.query(
                f"SELECT count(), sum(id) FROM {table}",
                user=user,
                settings={"maxcompute_read_format": "row", "odps_read_compress": 0},
            )
            == "8\t12\n"
        )
    finally:
        node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
        node1.query(f"DROP USER IF EXISTS {user}")


def test_granted_user_can_create_and_read_both_engine_names():
    user = table_name("MC-E2E-081-user")
    node1.query(f"DROP USER IF EXISTS {user}")
    try:
        node1.query(f"CREATE USER {user} IDENTIFIED WITH no_password")
        node1.query(f"GRANT CREATE TABLE, DROP TABLE, SELECT ON default.* TO {user}")
        node1.query(f"GRANT READ, WRITE ON MAXCOMPUTE TO {user}")
        for engine in ("MaxCompute", "MaxComputeRaw"):
            table = table_name("MC-E2E-081-" + engine)
            node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
            try:
                node1.query(
                    maxcompute_ddl(
                        table, "e2e_scalar", SCALAR_COLUMNS, engine=engine
                    ),
                    user=user,
                )
                assert (
                    node1.query(
                        f"SELECT count(), sum(id) FROM {table}",
                        user=user,
                        settings={
                            "maxcompute_read_format": "row",
                            "odps_read_compress": 0,
                        },
                    )
                    == "8\t12\n"
                )
            finally:
                node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
    finally:
        node1.query(f"DROP USER IF EXISTS {user}")


def test_named_collection_and_show_create_mask_secret():
    collection = table_name("MC-E2E-082-collection")
    table = table_name("MC-E2E-082-table")
    secret = "mc-named-secret-sentinel"
    node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node1.query(f"DROP NAMED COLLECTION IF EXISTS {collection}")
    try:
        node1.query(
            f"CREATE NAMED COLLECTION {collection} AS "
            f"endpoint = '{NORMAL_ENDPOINT}', project = 'test_project', "
            f"`table` = 'e2e_scalar', access_key_id = 'test-ak', "
            f"access_key_secret = '{secret}'"
        )
        node1.query(
            f"CREATE TABLE {table} ({SCALAR_COLUMNS}) "
            f"ENGINE = MaxComputeRaw({collection}) SETTINGS maxcompute_read_format = 'column'"
        )
        assert (
            node1.query(
                f"SELECT count(), sum(id) FROM {table}",
                settings={"maxcompute_read_format": "inherit", "odps_read_compress": 0},
            )
            == "8\t12\n"
        )
        assert secret not in node1.query(f"SHOW CREATE TABLE {table}")
    finally:
        node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
        node1.query(f"DROP NAMED COLLECTION IF EXISTS {collection}")


def test_positional_secret_is_absent_from_server_log():
    table = table_name("MC-E2E-083")
    secret = "mc-positional-secret-sentinel"
    node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
    try:
        node1.query(
            maxcompute_ddl(
                table,
                "e2e_scalar",
                SCALAR_COLUMNS,
                access_key_secret=secret,
            )
        )
        assert secret not in node1.query(f"SHOW CREATE TABLE {table}")
    finally:
        node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
    assert not node1.contains_in_log(secret)


@pytest.mark.parametrize(
    "endpoint,expected",
    [
        pytest.param("ftp://example.test/path", "must use HTTP or HTTPS", id="MC-E2E-085a"),
        pytest.param(
            "http://user:password@example.test",
            "must not contain user information",
            id="MC-E2E-085b",
        ),
        pytest.param(
            "http://example.test/path?secret=x",
            "must not contain a query string or fragment",
            id="MC-E2E-085c",
        ),
        pytest.param(
            "http://example.test/path#secret",
            "must not contain a query string or fragment",
            id="MC-E2E-085d",
        ),
    ],
)
def test_endpoint_validation(request, endpoint, expected):
    table = table_name(request.node.callspec.id)
    node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
    try:
        create_error = node1.query_and_get_error(
            maxcompute_ddl(table, "e2e_scalar", "id Int64", endpoint=endpoint)
        )
        error = create_error
        if not error:
            error = node1.query_and_get_error(f"SELECT count() FROM {table}")
        assert_error_contains(error, expected)
    finally:
        node1.query(f"DROP TABLE IF EXISTS {table} SYNC")


@pytest.mark.parametrize(
    "secret,access_key_id,expected",
    [
        pytest.param("test-sk", "test-ak", None, id="MC-E2E-090a"),
        pytest.param("wrong-secret", "test-ak", "Unauthorized", id="MC-E2E-090b"),
        pytest.param("limited-sk", "limited-ak", "NoPermission", id="MC-E2E-090c"),
    ],
)
def test_strict_authentication(request, secret, access_key_id, expected):
    with source_table(
        request.node.callspec.id,
        "e2e_scalar",
        SCALAR_COLUMNS,
        endpoint=STRICT_ENDPOINT,
        access_key_id=access_key_id,
        access_key_secret=secret,
    ) as table:
        settings = {"maxcompute_read_format": "row", "odps_read_compress": 0}
        if expected is None:
            assert node1.query(f"SELECT count(), sum(id) FROM {table}", settings=settings) == "8\t12\n"
        else:
            assert_error_contains(
                node1.query_and_get_error(f"SELECT count() FROM {table}", settings=settings),
                expected,
            )


@pytest.mark.parametrize(
    "quota,expected",
    [
        pytest.param("named", None, id="MC-E2E-091a"),
        pytest.param("missing", "QuotaNotExist", id="MC-E2E-091b"),
    ],
)
def test_named_quota(request, quota, expected):
    with source_table(
        request.node.callspec.id,
        "e2e_scalar",
        SCALAR_COLUMNS,
        quota=quota,
    ) as table:
        settings = {"maxcompute_read_format": "row", "odps_read_compress": 0}
        if expected is None:
            assert node1.query(f"SELECT count(), sum(id) FROM {table}", settings=settings) == "8\t12\n"
        else:
            assert_error_contains(
                node1.query_and_get_error(f"SELECT count() FROM {table}", settings=settings),
                expected,
            )


@pytest.mark.parametrize("table_format", [None, "column"])
def test_fault_rule_is_observed_and_deleted_before_recovery(table_format):
    rule_url = f"http://127.0.0.1:{cluster.maxcompute_port}/__test/faults/clickhouse-arrow"
    rule = {
        "match": {
            "action": "read",
            "project": "test_project",
            "table": "e2e_scalar",
            "format": "arrow",
            "attempt": 0,
        },
        "effect": {"type": "malformed_arrow"},
        "times": 1,
        "ttl_seconds": 60,
    }
    response = requests.put(rule_url, json=rule, timeout=5)
    response.raise_for_status()
    try:
        with source_table(
            "MC-E2E-092-fault", "e2e_scalar", SCALAR_COLUMNS, table_format=table_format
        ) as table:
            error = node1.query_and_get_error(
                f"SELECT count(), sum(id) FROM {table}",
                settings={
                    "maxcompute_read_format": "inherit",
                    "odps_read_compress": 0,
                    "maxcompute_max_retries": 0,
                },
            )
            assert error
        state_response = requests.get(rule_url, timeout=5)
        state_response.raise_for_status()
        state = state_response.json()
        assert state["hits"] == 1, state
        assert state["attempts"] >= state["hits"], state
    finally:
        delete_response = requests.delete(rule_url, timeout=5)
        delete_response.raise_for_status()

    with source_table(
        "MC-E2E-092-recovery", "e2e_scalar", SCALAR_COLUMNS, table_format=table_format
    ) as table:
        assert (
            node1.query(
                f"SELECT count(), sum(id) FROM {table}",
                settings={"maxcompute_read_format": "inherit", "odps_read_compress": 0},
            )
            == "8\t12\n"
        )


@pytest.mark.parametrize(
    "remote_table,columns,query,expected",
    [
        pytest.param(
            "e2e_types",
            "id Int64",
            "SELECT min(id), max(id), uniqExact(id) FROM {table}",
            "-9223372036854775808\t9223372036854775807\t3\n",
            id="MC-E2E-093a",
        ),
        pytest.param(
            "e2e_unsigned_boundary",
            "n Decimal128(0)",
            "SELECT toString(n) FROM {table}",
            "18446744073709551615\n",
            id="MC-E2E-093b",
        ),
    ],
)
def test_official_boundary_type_fixtures(
    request, remote_table, columns, query, expected
):
    with source_table(
        request.node.callspec.id,
        remote_table,
        columns,
        endpoint=TYPES_ENDPOINT,
    ) as table:
        assert (
            node1.query(
                query.format(table=table),
                settings={"maxcompute_read_format": "row", "odps_read_compress": 0},
            )
            == expected
        )
