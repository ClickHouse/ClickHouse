import os

import pytest

from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
DATABASE = "test_bloom_filter_context_restart"
INITIAL_CONFIG = os.path.join(SCRIPT_DIR, "configs/best_effort.xml")
CONFIGS = {
    "best_effort": INITIAL_CONFIG,
    "best_effort_us": os.path.join(SCRIPT_DIR, "configs/best_effort_us.xml"),
}
FLOAT_CONFIGS = {
    0: os.path.join(SCRIPT_DIR, "configs/precise_float_parsing_0.xml"),
    1: os.path.join(SCRIPT_DIR, "configs/precise_float_parsing_1.xml"),
}
FLOAT_CONFIG_DESTINATION = "/etc/clickhouse-server/users.d/precise_float_parsing.xml"
FLOAT_DATABASE = f"{DATABASE}_float"
FLOAT_TEXT = "0.1e-5"
TIMEZONE_CONFIGS = {
    "UTC": os.path.join(SCRIPT_DIR, "configs/timezone_utc.xml"),
    "America/Los_Angeles": os.path.join(SCRIPT_DIR, "configs/timezone_los_angeles.xml"),
}
TIMEZONE_CONFIG_DESTINATION = "/etc/clickhouse-server/config.d/timezone_utc.xml"
EXPECTED_ROWS = "[" + ",".join(str(i) for i in range(16)) + "]"
EXPECTED_TIMEZONE_ROW = "[1]"
NATIVE_FAMILIES = ("datetime", "datetime32", "datetime64", "unix")
DATE_TEXT = "01/02/2020 20:00:00"
QUERY_SETTINGS = {
    "explain_query_plan_default": "legacy",
    "session_timezone": "UTC",
    "use_query_condition_cache": 0,
    "use_skip_indexes": 1,
    "use_skip_indexes_on_data_read": 0,
}
TIMEZONE_QUERY_SETTINGS = {
    "explain_query_plan_default": "legacy",
    "use_query_condition_cache": 0,
    "use_skip_indexes": 1,
    "use_skip_indexes_on_data_read": 0,
}

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/timezone_utc.xml"],
    user_configs=["configs/best_effort.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _create_tables(date_time_type):
    node.query(f"CREATE DATABASE {DATABASE} ENGINE = Atomic")
    node.query(f"""
        CREATE TABLE {DATABASE}.scalar_cast
        (
            id UInt64,
            s String,
            INDEX idx CAST(s AS {date_time_type})
                TYPE bloom_filter(0.000001) GRANULARITY 1
        )
        ENGINE = MergeTree
        ORDER BY id
        SETTINGS index_granularity = 1, index_granularity_bytes = 0
        """)
    node.query(f"""
        CREATE TABLE {DATABASE}.array_map_cast
        (
            id UInt64,
            a Array(String),
            INDEX idx arrayMap(x -> CAST(x AS {date_time_type}), a)
                TYPE bloom_filter(0.000001) GRANULARITY 1
        )
        ENGINE = MergeTree
        ORDER BY id
        SETTINGS index_granularity = 1, index_granularity_bytes = 0
        """)
    node.query(f"""
        CREATE TABLE {DATABASE}.array_whole_cast
        (
            id UInt64,
            a Array(String),
            INDEX idx CAST(a AS Array({date_time_type}))
                TYPE bloom_filter(0.000001) GRANULARITY 1
        )
        ENGINE = MergeTree
        ORDER BY id
        SETTINGS index_granularity = 1, index_granularity_bytes = 0
        """)
    node.query(f"""
        CREATE TABLE {DATABASE}.numeric_cast
        (
            id UInt64,
            n UInt64,
            INDEX idx toUInt8(n) TYPE bloom_filter(0.000001) GRANULARITY 1
        )
        ENGINE = MergeTree
        ORDER BY id
        SETTINGS
            index_granularity = 1,
            index_granularity_bytes = 0,
            min_rows_for_wide_part = 0,
            min_bytes_for_wide_part = 0,
            add_minmax_index_for_numeric_columns = 0
        """)

    node.query(
        f"INSERT INTO {DATABASE}.scalar_cast "
        "SELECT number, '01/02/2020 20:00:00' FROM numbers(16)"
    )
    node.query(
        f"INSERT INTO {DATABASE}.array_map_cast "
        "SELECT number, ['01/02/2020 20:00:00'] FROM numbers(16)"
    )
    node.query(
        f"INSERT INTO {DATABASE}.array_whole_cast "
        "SELECT number, ['01/02/2020 20:00:00'] FROM numbers(16)"
    )
    node.query(
        f"INSERT INTO {DATABASE}.numeric_cast SELECT number, number FROM numbers(16)"
    )

    _create_native_tables()


def _native_conversion(family, value):
    if family == "datetime":
        return f"toDateTime({value}, 'UTC')"
    if family == "datetime32":
        return f"toDateTime32({value}, 'UTC')"
    if family == "datetime64":
        return f"toDateTime64({value}, 3, 'UTC')"
    assert family == "unix"
    return f"toUnixTimestamp({value}, 'UTC')"


def _native_target(family, mode):
    value = {
        "best_effort": "2020-02-01 20:00:00",
        "best_effort_us": "2020-01-02 20:00:00",
    }[mode]
    return _native_conversion(family, f"'{value}'")


def _native_expected_output(family, mode):
    values = {
        "best_effort": {
            "datetime": "2020-02-01 20:00:00",
            "datetime32": "2020-02-01 20:00:00",
            "datetime64": "2020-02-01 20:00:00.000",
            "unix": "1580587200",
        },
        "best_effort_us": {
            "datetime": "2020-01-02 20:00:00",
            "datetime32": "2020-01-02 20:00:00",
            "datetime64": "2020-01-02 20:00:00.000",
            "unix": "1577995200",
        },
    }
    return values[mode][family]


def _assert_native_outputs(mode):
    for family in NATIVE_FAMILIES:
        expression = _native_conversion(family, "s")
        actual = node.query(
            f"SELECT toString({expression}) FROM {DATABASE}.native_{family}_string LIMIT 1"
        ).strip()
        assert actual == _native_expected_output(family, mode)

        if family == "unix":
            fixed_actual = node.query(
                "SELECT toString(toUnixTimestamp(s, 'UTC')) "
                f"FROM {DATABASE}.native_unix_fixedstring_numeric LIMIT 1"
            ).strip()
            assert fixed_actual == "1580587200"
        else:
            fixed_actual = node.query(
                f"SELECT toString({expression}) "
                f"FROM {DATABASE}.native_{family}_fixedstring LIMIT 1"
            ).strip()
            assert fixed_actual == _native_expected_output(family, mode)


def _assert_native_context_indexes_declined(mode):
    for family in NATIVE_FAMILIES:
        target = _native_target(family, mode)
        scalar_expression = _native_conversion(family, "s")
        scalar_table = f"native_{family}_string"
        for predicate in (
            f"{scalar_expression} = {target}",
            f"{scalar_expression} IN ({target})",
        ):
            _assert_unsafe_index_declined(
                f"{DATABASE}.{scalar_table} WHERE {predicate}"
            )
            _assert_indexed_rows_match_full_scan(scalar_table, predicate)

        array_expression = _native_conversion(family, "x")
        array_table = f"native_{family}_array"
        for predicate in (
            f"has(arrayMap(x -> {array_expression}, a), {target})",
            f"arrayMap(x -> {array_expression}, a) IN ([{target}])",
        ):
            _assert_unsafe_index_declined(f"{DATABASE}.{array_table} WHERE {predicate}")
            _assert_indexed_rows_match_full_scan(array_table, predicate)

        if family == "unix":
            # toUnixTimestamp(FixedString) parses numeric input; its ambiguous
            # date-text case is invalid and is not used as a mode-change oracle.
            fixed_table = "native_unix_fixedstring_numeric"
            fixed_target = "toUInt32(1580587200)"
        else:
            fixed_table = f"native_{family}_fixedstring"
            fixed_target = target
        fixed_expression = _native_conversion(family, "s")
        for predicate in (
            f"{fixed_expression} = {fixed_target}",
            f"{fixed_expression} IN ({fixed_target})",
        ):
            _assert_unsafe_index_declined(f"{DATABASE}.{fixed_table} WHERE {predicate}")
            _assert_indexed_rows_match_full_scan(fixed_table, predicate)


def _assert_numeric_datetime_indexes_selected():
    for family, expression, target in (
        ("datetime", "toDateTime(n, 'UTC')", "toDateTime(2, 'UTC')"),
        ("unix", "toUnixTimestamp(n, 'UTC')", "toUnixTimestamp(2, 'UTC')"),
    ):
        table = f"numeric_{family}"
        predicate = f"{expression} = {target}"
        plan = node.query(
            f"EXPLAIN indexes = 1 SELECT id FROM {DATABASE}.{table} "
            f"WHERE {predicate}",
            settings=QUERY_SETTINGS,
        )
        assert "Name: idx" in plan, plan
        assert "Granules: 1/16" in plan, plan
        _assert_indexed_rows_match_full_scan(table, predicate, "[2]")


def _create_native_table(table, column, expression, inserted_value):
    node.query(f"""
        CREATE TABLE {DATABASE}.{table}
        (
            id UInt64,
            {column},
            INDEX idx {expression} TYPE bloom_filter(0.000001) GRANULARITY 1
        )
        ENGINE = MergeTree
        ORDER BY id
        SETTINGS
            index_granularity = 1,
            index_granularity_bytes = 0,
            min_rows_for_wide_part = 0,
            min_bytes_for_wide_part = 0,
            add_minmax_index_for_numeric_columns = 0
        """)
    node.query(
        f"INSERT INTO {DATABASE}.{table} "
        f"SELECT number, {inserted_value} FROM numbers(16)"
    )


def _create_native_tables():
    for family in NATIVE_FAMILIES:
        scalar_expression = _native_conversion(family, "s")
        _create_native_table(
            f"native_{family}_string",
            "s String",
            scalar_expression,
            f"'{DATE_TEXT}'",
        )

        array_expression = _native_conversion(family, "x")
        node.query(f"""
            CREATE TABLE {DATABASE}.native_{family}_array
            (
                id UInt64,
                a Array(String),
                INDEX idx arrayMap(x -> {array_expression}, a)
                    TYPE bloom_filter(0.000001) GRANULARITY 1
            )
            ENGINE = MergeTree
            ORDER BY id
            SETTINGS
                index_granularity = 1,
                index_granularity_bytes = 0,
                min_rows_for_wide_part = 0,
                min_bytes_for_wide_part = 0,
                add_minmax_index_for_numeric_columns = 0
            """)
        node.query(
            f"INSERT INTO {DATABASE}.native_{family}_array "
            f"SELECT number, ['{DATE_TEXT}'] FROM numbers(16)"
        )

        if family == "unix":
            _create_native_table(
                "native_unix_fixedstring_numeric",
                "s FixedString(10)",
                scalar_expression,
                "toFixedString('1580587200', 10)",
            )
        else:
            _create_native_table(
                f"native_{family}_fixedstring",
                "s FixedString(19)",
                scalar_expression,
                f"toFixedString('{DATE_TEXT}', 19)",
            )

    for family, expression in (
        ("datetime", "toDateTime(n, 'UTC')"),
        ("unix", "toUnixTimestamp(n, 'UTC')"),
    ):
        node.query(f"""
            CREATE TABLE {DATABASE}.numeric_{family}
            (
                id UInt64,
                n UInt32,
                INDEX idx {expression} TYPE bloom_filter(0.000001) GRANULARITY 1
            )
            ENGINE = MergeTree
            ORDER BY id
            SETTINGS
                index_granularity = 1,
                index_granularity_bytes = 0,
                min_rows_for_wide_part = 0,
                min_bytes_for_wide_part = 0,
                add_minmax_index_for_numeric_columns = 0
            """)
        node.query(
            f"INSERT INTO {DATABASE}.numeric_{family} "
            "SELECT number, number FROM numbers(16)"
        )


def _date_target(date_time_type, mode):
    value = {
        "best_effort": "2020-02-01 20:00:00",
        "best_effort_us": "2020-01-02 20:00:00",
    }[mode]
    if date_time_type.startswith("DateTime64"):
        return f"toDateTime64('{value}', 3, 'UTC')"
    return f"toDateTime('{value}', 'UTC')"


def _assert_unsafe_index_declined(predicate, settings=QUERY_SETTINGS):
    plan = node.query(
        f"EXPLAIN indexes = 1 SELECT id FROM {predicate}", settings=settings
    )
    assert "Name: idx" not in plan, plan


def _assert_indexed_rows_match_full_scan(
    table,
    predicate,
    expected_rows=EXPECTED_ROWS,
    settings=QUERY_SETTINGS,
    database=DATABASE,
):
    query = (
        f"SELECT arraySort(groupArray(id)) "
        f"FROM {database}.{table} WHERE {predicate}"
    )
    indexed = node.query(query, settings=settings).strip()
    full_scan_settings = dict(settings, use_skip_indexes=0)
    full_scan = node.query(query, settings=full_scan_settings).strip()

    assert indexed == full_scan
    assert indexed == expected_rows


def _float_context():
    actual = node.query(
        "SELECT toUInt8(getSetting('precise_float_parsing')), "
        "reinterpretAsUInt64(CAST('0.1e-5' AS Float64))"
    ).strip()
    setting, bits = map(int, actual.split("\t"))
    return setting, bits


def _create_float_index_tables(database):
    node.query(f"DROP DATABASE IF EXISTS {database} SYNC")
    node.query(f"CREATE DATABASE {database} ENGINE = Atomic")
    tables = (
        (
            "cast_scalar",
            "s String",
            "CAST(s AS Float64)",
            f"'{FLOAT_TEXT}'",
        ),
        (
            "native_scalar",
            "s String",
            "toFloat64(s)",
            f"'{FLOAT_TEXT}'",
        ),
        (
            "cast_array",
            "a Array(String)",
            "CAST(a AS Array(Float64))",
            f"['{FLOAT_TEXT}']",
        ),
        (
            "array_map",
            "a Array(String)",
            "arrayMap(x -> CAST(x AS Float64), a)",
            f"['{FLOAT_TEXT}']",
        ),
        (
            "named_tuple",
            "t Tuple(a Float64, b String)",
            "tupleElement(CAST(t AS Tuple(b Float64, a String)), 1)",
            f"tuple(toFloat64(0), '{FLOAT_TEXT}')",
        ),
        (
            "folded_cast",
            "n Float64, a Array(Float64)",
            f"plus(n, CAST('{FLOAT_TEXT}' AS Float64))",
            "toFloat64(0), [toFloat64(0)]",
        ),
        (
            "folded_native",
            "n Float64, a Array(Float64)",
            f"plus(n, toFloat64('{FLOAT_TEXT}'))",
            "toFloat64(0), [toFloat64(0)]",
        ),
        (
            "folded_lambda",
            "n Float64, a Array(Float64)",
            f"arrayMap(x -> plus(x, CAST('{FLOAT_TEXT}' AS Float64)), a)",
            "toFloat64(0), [toFloat64(0)]",
        ),
    )
    for table, source_column, expression, inserted_value in tables:
        node.query(f"""
            CREATE TABLE {database}.{table}
            (
                id UInt64,
                {source_column},
                INDEX idx {expression} TYPE bloom_filter(0.000001) GRANULARITY 1
            )
            ENGINE = MergeTree
            ORDER BY id
            SETTINGS
                index_granularity = 1,
                index_granularity_bytes = 0,
                min_rows_for_wide_part = 0,
                min_bytes_for_wide_part = 0
            """)
        node.query(
            f"INSERT INTO {database}.{table} "
            f"SELECT number, {inserted_value} FROM numbers(16)"
        )

    node.query(f"""
        CREATE TABLE {database}.numeric_cast
        (
            id UInt64,
            n UInt64,
            INDEX idx toUInt8(n) TYPE bloom_filter(0.000001) GRANULARITY 1
        )
        ENGINE = MergeTree
        ORDER BY id
        SETTINGS
            index_granularity = 1,
            index_granularity_bytes = 0,
            min_rows_for_wide_part = 0,
            min_bytes_for_wide_part = 0,
            add_minmax_index_for_numeric_columns = 0
        """)
    node.query(
        f"INSERT INTO {database}.numeric_cast " "SELECT number, number FROM numbers(16)"
    )


def _assert_float_context_indexes_declined(database, target):
    cases = (
        (
            "cast_scalar",
            f"CAST(s AS Float64) = {target}",
        ),
        (
            "cast_scalar",
            f"CAST(s AS Float64) IN ({target})",
        ),
        (
            "native_scalar",
            f"toFloat64(s) = {target}",
        ),
        (
            "cast_array",
            f"has(CAST(a AS Array(Float64)), {target})",
        ),
        (
            "array_map",
            "has(arrayMap(x -> CAST(x AS Float64), a), " f"{target})",
        ),
        (
            "named_tuple",
            f"tupleElement(CAST(t AS Tuple(b Float64, a String)), 1) = {target}",
        ),
        (
            "named_tuple",
            f"tupleElement(CAST(t AS Tuple(b Float64, a String)), 1) IN ({target})",
        ),
        (
            "folded_cast",
            f"plus(n, CAST('{FLOAT_TEXT}' AS Float64)) = {target}",
        ),
        (
            "folded_cast",
            f"plus(n, CAST('{FLOAT_TEXT}' AS Float64)) IN ({target})",
        ),
        (
            "folded_native",
            f"plus(n, toFloat64('{FLOAT_TEXT}')) IN ({target})",
        ),
        (
            "folded_lambda",
            f"has(arrayMap(x -> plus(x, CAST('{FLOAT_TEXT}' AS Float64)), a), {target})",
        ),
    )
    for table, predicate in cases:
        qualified_predicate = f"{database}.{table} WHERE {predicate}"
        _assert_unsafe_index_declined(qualified_predicate)
        _assert_indexed_rows_match_full_scan(
            table,
            predicate,
            EXPECTED_ROWS,
            database=database,
        )

    numeric_predicate = "toUInt8(n) IN (2, 12)"
    numeric_plan = node.query(
        f"EXPLAIN indexes = 1 SELECT id FROM {database}.numeric_cast "
        f"WHERE {numeric_predicate}",
        settings=QUERY_SETTINGS,
    )
    assert "Name: idx" in numeric_plan, numeric_plan
    assert "Granules: 2/16" in numeric_plan, numeric_plan
    _assert_indexed_rows_match_full_scan(
        "numeric_cast",
        numeric_predicate,
        "[2,12]",
        database=database,
    )


@pytest.mark.parametrize(
    "build_mode,reload_mode",
    [(0, 0), (0, 1), (1, 0), (1, 1)],
)
def test_persisted_bloom_filter_respects_float_context_restart(build_mode, reload_mode):
    try:
        node.copy_file_to_container(FLOAT_CONFIGS[build_mode], FLOAT_CONFIG_DESTINATION)
        node.restart_clickhouse()
        actual_build_mode, build_bits = _float_context()
        assert actual_build_mode == build_mode

        _create_float_index_tables(FLOAT_DATABASE)

        node.copy_file_to_container(
            FLOAT_CONFIGS[reload_mode], FLOAT_CONFIG_DESTINATION
        )
        node.restart_clickhouse()
        actual_reload_mode, reload_bits = _float_context()
        assert actual_reload_mode == reload_mode
        if build_mode == reload_mode:
            assert build_bits == reload_bits
        else:
            assert build_bits != reload_bits

        target = "reinterpretAsFloat64(toUInt64(" f"{reload_bits}))"
        _assert_float_context_indexes_declined(FLOAT_DATABASE, target)
    finally:
        try:
            node.query(f"DROP DATABASE IF EXISTS {FLOAT_DATABASE} SYNC")
        finally:
            node.copy_file_to_container(FLOAT_CONFIGS[0], FLOAT_CONFIG_DESTINATION)
            node.restart_clickhouse()
            actual_mode, _ = _float_context()
            assert actual_mode == 0


@pytest.mark.parametrize(
    "date_time_type",
    [
        pytest.param("DateTime('UTC')", id="datetime"),
        pytest.param("DateTime64(3, 'UTC')", id="datetime64"),
    ],
)
@pytest.mark.parametrize("restart_mode", ["best_effort", "best_effort_us"])
def test_persisted_bloom_filter_respects_cast_context_restart(
    date_time_type, restart_mode
):
    try:
        assert (
            node.query("SELECT getSetting('cast_string_to_date_time_mode')").strip()
            == "best_effort"
        )

        _create_tables(date_time_type)
        _assert_native_outputs("best_effort")
        before_restart = node.query(
            f"SELECT formatDateTime(CAST(s AS {date_time_type}), '%F %T') "
            f"FROM {DATABASE}.scalar_cast LIMIT 1"
        ).strip()
        assert before_restart == "2020-02-01 20:00:00"

        node.copy_file_to_container(
            CONFIGS[restart_mode],
            "/etc/clickhouse-server/users.d/best_effort.xml",
        )
        node.restart_clickhouse()

        assert (
            node.query("SELECT getSetting('cast_string_to_date_time_mode')").strip()
            == restart_mode
        )
        after_restart = node.query(
            f"SELECT formatDateTime(CAST(s AS {date_time_type}), '%F %T') "
            f"FROM {DATABASE}.scalar_cast LIMIT 1"
        ).strip()
        expected_value = {
            "best_effort": "2020-02-01 20:00:00",
            "best_effort_us": "2020-01-02 20:00:00",
        }[restart_mode]
        assert after_restart == expected_value
        _assert_native_outputs(restart_mode)

        target = _date_target(date_time_type, restart_mode)
        unsafe_cases = [
            (
                "scalar_cast",
                f"CAST(s AS {date_time_type}) = {target}",
            ),
            (
                "scalar_cast",
                f"CAST(s AS {date_time_type}) IN ({target})",
            ),
            (
                "array_map_cast",
                f"has(arrayMap(x -> CAST(x AS {date_time_type}), a), {target})",
            ),
            (
                "array_map_cast",
                f"arrayMap(x -> CAST(x AS {date_time_type}), a) IN ([{target}])",
            ),
            (
                "array_whole_cast",
                f"has(CAST(a AS Array({date_time_type})), {target})",
            ),
            (
                "array_map_cast",
                "arrayJoin(CAST(CAST("
                f"arrayMap(x -> CAST(x AS {date_time_type}), a) AS Dynamic) "
                f"AS Array({date_time_type}))) = {target}",
            ),
            (
                "array_map_cast",
                "arrayJoin(CAST(CAST("
                f"arrayMap(x -> CAST(x AS {date_time_type}), a) AS Dynamic) "
                f"AS Array({date_time_type}))) IN ({target})",
            ),
        ]

        for table, predicate in unsafe_cases:
            _assert_unsafe_index_declined(f"{DATABASE}.{table} WHERE {predicate}")
            _assert_indexed_rows_match_full_scan(table, predicate)

        _assert_native_context_indexes_declined(restart_mode)
        _assert_numeric_datetime_indexes_selected()

        numeric_plan = node.query(
            f"EXPLAIN indexes = 1 SELECT id FROM {DATABASE}.numeric_cast "
            "WHERE toUInt8(n) IN (2, 12)",
            settings=QUERY_SETTINGS,
        )
        assert "Name: idx" in numeric_plan, numeric_plan
        assert "Granules: 2/16" in numeric_plan, numeric_plan
        _assert_indexed_rows_match_full_scan(
            "numeric_cast", "toUInt8(n) IN (2, 12)", "[2,12]"
        )
        assert (
            node.query(
                f"SELECT arraySort(groupArray(id)) FROM {DATABASE}.numeric_cast "
                "WHERE toUInt8(n) IN (2, 12)",
                settings=QUERY_SETTINGS,
            ).strip()
            == "[2,12]"
        )
    finally:
        try:
            node.query(f"DROP DATABASE IF EXISTS {DATABASE} SYNC")
        finally:
            node.copy_file_to_container(
                INITIAL_CONFIG,
                "/etc/clickhouse-server/users.d/best_effort.xml",
            )
            node.restart_clickhouse()
            assert (
                node.query("SELECT getSetting('cast_string_to_date_time_mode')").strip()
                == "best_effort"
            )


def _create_timezone_table(
    database, table, source_column, index_expression, inserted_value
):
    node.query(f"""
        CREATE TABLE {database}.{table}
        (
            id UInt64,
            {source_column},
            INDEX idx {index_expression} TYPE bloom_filter(0.000001) GRANULARITY 1
        )
        ENGINE = MergeTree
        ORDER BY id
        SETTINGS
            index_granularity = 1,
            index_granularity_bytes = 0,
            min_rows_for_wide_part = 0,
            min_bytes_for_wide_part = 0,
            add_minmax_index_for_numeric_columns = 0
        """)
    node.query(
        f"INSERT INTO {database}.{table} "
        f"SELECT number, {inserted_value} FROM numbers(16)"
    )


def _create_timezone_index_tables(database):
    node.query(f"DROP DATABASE IF EXISTS {database} SYNC")
    node.query(f"CREATE DATABASE {database} ENGINE = Atomic")
    cases = (
        (
            "cast_date_datetime",
            "d Date",
            "CAST(d AS DateTime)",
            "toDate('2020-01-01') + number",
            "CAST(d AS DateTime) = toDateTime('2020-01-02 00:00:00')",
        ),
        (
            "cast_date32_datetime",
            "d32 Date32",
            "CAST(d32 AS DateTime)",
            "toDate32('2020-01-01') + toInt32(number)",
            "CAST(d32 AS DateTime) = toDateTime('2020-01-02 00:00:00')",
        ),
        (
            "cast_date_datetime64",
            "d Date",
            "CAST(d AS DateTime64(3))",
            "toDate('2020-01-01') + number",
            "CAST(d AS DateTime64(3)) = toDateTime64('2020-01-02 00:00:00', 3)",
        ),
        (
            "cast_date32_datetime64",
            "d32 Date32",
            "CAST(d32 AS DateTime64(3))",
            "toDate32('2020-01-01') + toInt32(number)",
            "CAST(d32 AS DateTime64(3)) = toDateTime64('2020-01-02 00:00:00', 3)",
        ),
        (
            "native_date_datetime",
            "d Date",
            "toDateTime(d)",
            "toDate('2020-01-01') + number",
            "toDateTime(d) = toDateTime('2020-01-02 00:00:00')",
        ),
        (
            "native_date32_datetime",
            "d32 Date32",
            "toDateTime(d32)",
            "toDate32('2020-01-01') + toInt32(number)",
            "toDateTime(d32) = toDateTime('2020-01-02 00:00:00')",
        ),
    )
    for table, source_column, expression, inserted_value, _ in cases:
        _create_timezone_table(
            database, table, source_column, expression, inserted_value
        )

    _create_timezone_table(
        database,
        "numeric_datetime",
        "n UInt32",
        "toDateTime(n, 'UTC')",
        "number",
    )
    return cases


def _assert_timezone_context(timezone_name, expected_seconds, expected_milliseconds):
    actual = node.query(
        """
        SELECT
            timezone(),
            concat('<', getSetting('session_timezone'), '>'),
            toUInt64(CAST(toDate('2020-01-02') AS DateTime)),
            toUInt64(CAST(toDate32('2020-01-02') AS DateTime)),
            toUnixTimestamp64Milli(CAST(toDate('2020-01-02') AS DateTime64(3))),
            toUnixTimestamp64Milli(CAST(toDate32('2020-01-02') AS DateTime64(3)))
        """,
        settings=TIMEZONE_QUERY_SETTINGS,
    ).strip()
    expected = (
        f"{timezone_name}\t<>\t{expected_seconds}\t{expected_seconds}\t"
        f"{expected_milliseconds}\t{expected_milliseconds}"
    )
    assert actual == expected, (actual, expected)


def _assert_timezone_phase(cases, database, expected_plan_state):
    for table, _, _, _, predicate in cases:
        qualified_predicate = f"{database}.{table} WHERE {predicate}"
        plan = node.query(
            f"EXPLAIN indexes = 1 SELECT id FROM {qualified_predicate}",
            settings=TIMEZONE_QUERY_SETTINGS,
        )
        if expected_plan_state == "declined":
            assert "Name: idx" not in plan, plan
        else:
            assert "Name: idx" in plan, plan
        _assert_indexed_rows_match_full_scan(
            table,
            predicate,
            EXPECTED_TIMEZONE_ROW,
            settings=TIMEZONE_QUERY_SETTINGS,
            database=database,
        )

    numeric_predicate = "toDateTime(n, 'UTC') = toDateTime(1, 'UTC')"
    numeric_plan = node.query(
        f"EXPLAIN indexes = 1 SELECT id FROM {database}.numeric_datetime "
        f"WHERE {numeric_predicate}",
        settings=TIMEZONE_QUERY_SETTINGS,
    )
    assert "Name: idx" in numeric_plan, numeric_plan
    assert "Granules: 1/16" in numeric_plan, numeric_plan
    _assert_indexed_rows_match_full_scan(
        "numeric_datetime",
        numeric_predicate,
        EXPECTED_TIMEZONE_ROW,
        settings=TIMEZONE_QUERY_SETTINGS,
        database=database,
    )


def test_date_cast_bloom_index_is_safe_across_server_timezone_restart():
    database = f"{DATABASE}_timezone"
    try:
        _assert_timezone_context("UTC", 1577923200, 1577923200000)
        cases = _create_timezone_index_tables(database)
        _assert_timezone_phase(cases, database, "declined")

        node.copy_file_to_container(
            TIMEZONE_CONFIGS["America/Los_Angeles"], TIMEZONE_CONFIG_DESTINATION
        )
        node.restart_clickhouse()

        _assert_timezone_context("America/Los_Angeles", 1577952000, 1577952000000)
        _assert_timezone_phase(cases, database, "declined")
    finally:
        try:
            node.copy_file_to_container(
                TIMEZONE_CONFIGS["UTC"], TIMEZONE_CONFIG_DESTINATION
            )
            node.restart_clickhouse()
            _assert_timezone_context("UTC", 1577923200, 1577923200000)
        finally:
            node.query(f"DROP DATABASE IF EXISTS {database} SYNC")
