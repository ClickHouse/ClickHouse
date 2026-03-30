#!/usr/bin/env bash
# Tags: no-fasttest
# Parquet filter pushdown should not use min/max statistics for Dynamic/Variant/JSON columns.
# These column types can hold values of different types, so Parquet physical-type statistics
# (based on the storage type, usually String) cannot be used for row group filtering.
# Without this fix, String statistics compared with non-String KeyCondition constants
# can produce incorrect results or throw BAD_TYPE_OF_FIELD.
# https://github.com/ClickHouse/ClickHouse/issues/87695

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

opts=(
    --input_format_parquet_filter_push_down=1
)

# Create a Parquet file with 2 row groups of String data.
# Row group 1: "a" (50 rows), stats: min="a", max="a"
# Row group 2: "b" (50 rows), stats: min="b", max="b"
${CLICKHOUSE_CLIENT} "${opts[@]}" --query="
    INSERT INTO FUNCTION file('04050_dynamic_${CLICKHOUSE_DATABASE}.parquet')
    SELECT if(number < 50, 'a', 'b') AS c0 FROM numbers(100)
    SETTINGS output_format_parquet_row_group_size = 50, engine_file_truncate_on_insert = 1
"

# Query with a value outside the String stats range.
# On master (before fix): String stats are used for Dynamic columns, so both row groups
# are pruned because 'z' > max("a") and 'z' > max("b"). ParquetPrunedRowGroups = 2.
# After fix: stats-based filtering is skipped for Dynamic, so no row groups are pruned.
query_id="${CLICKHOUSE_DATABASE}_04050_dynamic_prune_${RANDOM}"
${CLICKHOUSE_CLIENT} "${opts[@]}" --query_id="${query_id}" --query="
    SELECT count() FROM file('04050_dynamic_${CLICKHOUSE_DATABASE}.parquet', Parquet, 'c0 Dynamic') WHERE c0 = 'z'
"

${CLICKHOUSE_CLIENT} -nq "
    SYSTEM FLUSH LOGS query_log;

    SELECT ProfileEvents['ParquetPrunedRowGroups']
    FROM system.query_log
    WHERE event_date >= yesterday()
      AND event_time >= now() - 600
      AND query_id = '${query_id}'
      AND type = 'QueryFinish'
      AND current_database = currentDatabase();
"

# Correctness tests: these should work regardless of filter pushdown.
${CLICKHOUSE_CLIENT} "${opts[@]}" --query="
    INSERT INTO FUNCTION file('04050_dynamic2_${CLICKHOUSE_DATABASE}.parquet')
    SELECT toString(number) AS c0 FROM numbers(100)
    SETTINGS engine_file_truncate_on_insert = 1
"

# Dynamic column with JSON comparison: NO_COMMON_TYPE because String and JSON have no common supertype.
${CLICKHOUSE_CLIENT} "${opts[@]}" --query="
    SELECT count() FROM file('04050_dynamic2_${CLICKHOUSE_DATABASE}.parquet', Parquet, 'c0 Dynamic') WHERE c0 = '{\"c1\":1}'::JSON
" 2>&1 | grep -o -m1 'NO_COMMON_TYPE'

# Dynamic column with String comparison.
${CLICKHOUSE_CLIENT} "${opts[@]}" --query="
    SELECT count() FROM file('04050_dynamic2_${CLICKHOUSE_DATABASE}.parquet', Parquet, 'c0 Dynamic') WHERE c0 = '1'
"

# Variant column with String comparison.
${CLICKHOUSE_CLIENT} "${opts[@]}" --query="
    SELECT count() FROM file('04050_dynamic2_${CLICKHOUSE_DATABASE}.parquet', Parquet, 'c0 Variant(String, UInt64)') WHERE c0 = '1'
"

# JSON column: verify no exception from filter pushdown.
${CLICKHOUSE_CLIENT} "${opts[@]}" --query="
    INSERT INTO FUNCTION file('04050_json_${CLICKHOUSE_DATABASE}.parquet')
    SELECT '{\"value\":' || toString(number) || '}' AS c0 FROM numbers(100)
    SETTINGS engine_file_truncate_on_insert = 1
"

${CLICKHOUSE_CLIENT} "${opts[@]}" --query="
    SELECT count() FROM file('04050_json_${CLICKHOUSE_DATABASE}.parquet', Parquet, 'c0 JSON') WHERE c0 IS NOT NULL
"
