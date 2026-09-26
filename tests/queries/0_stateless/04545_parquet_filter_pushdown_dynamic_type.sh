#!/usr/bin/env bash
# Tags: no-fasttest
# Regression test for https://github.com/ClickHouse/ClickHouse/issues/87695
# Parquet filter pushdown must not use row-group min/max or page-index statistics for
# Dynamic, Object (JSON), and Variant columns. Their physical Parquet type is String, but a
# row may logically hold a value of a different type, so the String stats are not meaningful.
# When pushdown was enabled the reader decoded those String stats and compared them against a
# non-String KeyCondition constant inside getHyperrectangleForRowGroup, throwing
# BAD_TYPE_OF_FIELD ("Cannot compare std::string with DB::Object"). The fix disables stats for
# these types (allow_stats = is_output_type_string()), so the row group is never pruned on
# physical String metadata and the comparison is left to query execution. The page-index path
# is gated on the same allow_stats flag (Reader.cpp), so it is covered too.
#
# This test targets the allow_stats-gated row-group / page-index path only. It does not cover
# bloom-filter pruning: Reader::prepareBloomFilterCondition hashes the predicate literal via
# parquetTryHashField, and for a BYTE_ARRAY column that helper only accepts Field::String; with
# the ::JSON constants below it returns nullopt and bloom pruning is skipped rather than throwing.
#
# Each query below feeds the reader-stats path a JSON constant, the exact #87695 trigger. If
# stats were re-enabled for these types the reader would again throw BAD_TYPE_OF_FIELD; with
# the fix it either returns a count (row group kept, comparison done at execution) or reports
# the ordinary NO_COMMON_TYPE at execution time. Every branch therefore changes output if the
# bug is reintroduced. Same reader path DeltaLakeLocal uses to read Parquet.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

opts=(
    --input_format_parquet_filter_push_down=1
    --input_format_parquet_bloom_filter_push_down=1
    --input_format_parquet_page_filter_push_down=1
)

# String physical column, 2 row groups with well-separated min/max stats ('a' vs 'z').
${CLICKHOUSE_CLIENT} --query="
    INSERT INTO FUNCTION file('04545_${CLICKHOUSE_DATABASE}.parquet')
    SELECT if(number < 50, 'a', 'z') AS c0 FROM numbers(100)
    SETTINGS output_format_parquet_row_group_size = 50, engine_file_truncate_on_insert = 1
"

# JSON physical column (Object written as String), 2 row groups: {"v":"a"} then {"v":"z"}.
${CLICKHOUSE_CLIENT} --query="
    INSERT INTO FUNCTION file('04545_obj_${CLICKHOUSE_DATABASE}.parquet')
    SELECT concat('{\"v\":\"', if(number < 50, 'a', 'z'), '\"}') AS c0 FROM numbers(100)
    SETTINGS output_format_parquet_row_group_size = 50, engine_file_truncate_on_insert = 1
"

# Dynamic column vs JSON constant: the original repro. Stats are skipped, so the reader does
# not decode the String stats to compare with the JSON constant (no BAD_TYPE_OF_FIELD). The
# type mismatch surfaces as NO_COMMON_TYPE at execution instead.
${CLICKHOUSE_CLIENT} "${opts[@]}" --query="
    SELECT count() FROM file('04545_${CLICKHOUSE_DATABASE}.parquet', Parquet, 'c0 Dynamic') WHERE c0 = '{\"v\":\"z\"}'::JSON
" 2>&1 | grep -oE 'BAD_TYPE_OF_FIELD|NO_COMMON_TYPE|^[0-9]+$' | head -1

# Object (JSON) column vs JSON constant: both sides JSON, so this reaches execution and
# returns a count. The two row groups have distinct String stats ({"v":"a"} vs {"v":"z"}), so
# the count of 50 proves the second row group was NOT pruned on physical String metadata. If
# stats were re-enabled the reader would throw BAD_TYPE_OF_FIELD feeding the JSON constant into
# the String stats comparison.
${CLICKHOUSE_CLIENT} "${opts[@]}" --query="
    SELECT count() FROM file('04545_obj_${CLICKHOUSE_DATABASE}.parquet', Parquet, 'c0 JSON') WHERE c0 = '{\"v\":\"z\"}'::JSON
" 2>&1 | grep -oE 'BAD_TYPE_OF_FIELD|NO_COMMON_TYPE|^[0-9]+$' | head -1

# Variant(String, UInt64) column vs JSON constant: pushdown must be skipped because a Variant
# row can hold either alternative, so its physical String stats are unsafe. With stats skipped
# the reader does not compare the JSON constant against the String stats (no BAD_TYPE_OF_FIELD);
# the mismatch is reported as NO_COMMON_TYPE at execution.
${CLICKHOUSE_CLIENT} "${opts[@]}" --query="
    SELECT count() FROM file('04545_${CLICKHOUSE_DATABASE}.parquet', Parquet, 'c0 Variant(String, UInt64)') WHERE c0 = '{\"v\":\"z\"}'::JSON
" 2>&1 | grep -oE 'BAD_TYPE_OF_FIELD|NO_COMMON_TYPE|^[0-9]+$' | head -1
