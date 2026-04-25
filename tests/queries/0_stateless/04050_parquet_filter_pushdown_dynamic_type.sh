#!/usr/bin/env bash
# Tags: no-fasttest
# Parquet filter pushdown should not use min/max statistics or bloom filters for `Dynamic`,
# `Object` (JSON), and `Variant` columns. These column types can hold values of different types,
# so Parquet physical-type statistics (based on the storage type, usually String) are not
# meaningful for filtering, and comparing them with non-String `KeyCondition` constants
# can throw `BAD_TYPE_OF_FIELD` in `FieldVisitorAccurateLess`.
# The bug only manifests in the arrow-based reader (`input_format_parquet_use_native_reader_v3=0`).
# https://github.com/ClickHouse/ClickHouse/issues/87695

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

opts=(
    --input_format_parquet_filter_push_down=1
    --input_format_parquet_bloom_filter_push_down=1
    --input_format_parquet_use_native_reader_v3=0
)

# Plain String values, written as 2 row groups so that String physical-type min/max statistics
# are present for the column.
${CLICKHOUSE_CLIENT} --query="
    INSERT INTO FUNCTION file('04050_${CLICKHOUSE_DATABASE}.parquet')
    SELECT if(number < 50, 'a', 'b') AS c0 FROM numbers(100)
    SETTINGS output_format_parquet_row_group_size = 50, engine_file_truncate_on_insert = 1
"

# `Dynamic` column with a JSON-typed `KeyCondition` constant. On master, the arrow-based reader
# decoded the String parquet stats and tried to compare them with the JSON constant from the
# query, throwing `BAD_TYPE_OF_FIELD`. After the fix, stats-based pushdown is skipped for
# `Dynamic` columns, so the parquet reader does not throw; the comparison is then handled at
# query execution and reports the type mismatch as `NO_COMMON_TYPE`.
${CLICKHOUSE_CLIENT} "${opts[@]}" --query="
    SELECT count() FROM file('04050_${CLICKHOUSE_DATABASE}.parquet', Parquet, 'c0 Dynamic') WHERE c0 = '{\"v\":\"z\"}'::JSON
" 2>&1 | grep -oE 'BAD_TYPE_OF_FIELD|NO_COMMON_TYPE|^[0-9]+$' | head -1

# `Object` (JSON) column with a JSON-typed constant: both sides are JSON, no analyzer error.
# Without the fix, the bloom-filter path could feed a JSON `Field` into the String-typed Parquet
# bloom filter and fail. After the fix, the column is excluded from bloom-filter pushdown.
${CLICKHOUSE_CLIENT} --query="
    INSERT INTO FUNCTION file('04050_obj_${CLICKHOUSE_DATABASE}.parquet')
    SELECT concat('{\"v\":\"', if(number < 50, 'a', 'b'), '\"}') AS c0 FROM numbers(100)
    SETTINGS output_format_parquet_row_group_size = 50, engine_file_truncate_on_insert = 1
"

${CLICKHOUSE_CLIENT} "${opts[@]}" --query="
    SELECT count() FROM file('04050_obj_${CLICKHOUSE_DATABASE}.parquet', Parquet, 'c0 JSON') WHERE c0 = '{\"v\":\"z\"}'::JSON
"

# `Variant(String, UInt64)` with a String constant: the comparison types match, but pushdown
# should still be skipped because `Variant` rows can hold either alternative.
${CLICKHOUSE_CLIENT} "${opts[@]}" --query="
    SELECT count() FROM file('04050_${CLICKHOUSE_DATABASE}.parquet', Parquet, 'c0 Variant(String, UInt64)') WHERE c0 = 'z'
"
