#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: writes and reads Parquet files

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR=$(mktemp -d "${CLICKHOUSE_TMP}/05137_parquet_topk_XXXXXX")
trap 'rm -rf "${DIR}"' EXIT

# Cases where the Parquet min/max statistics do not bound a row group in the order or in the value
# space the TopN threshold lives in, so `Reader::topKShouldSkipRowGroup` must not consult them.
# Every query here answered differently with and without `use_top_k_dynamic_filtering` before the
# guards were added.
#
# Pinned: the top-k settings, the native Parquet reader (the only one that consumes the top-k
# filter), its stats-based pruning, and single-threaded reading - each case needs the first file of
# the glob to publish the threshold before the second file's row group is reached.
LOCAL=(${CLICKHOUSE_LOCAL}
    --use_top_k_dynamic_filtering_for_variable_length_types=0
    --query_plan_max_limit_for_top_k_optimization=1000
    --input_format_parquet_use_native_reader_v3=1
    --input_format_parquet_filter_push_down=1
    --max_block_size=65409
    --max_threads=1 --max_parsing_threads=1)
ON=(--use_top_k_dynamic_filtering=1)
OFF=(--use_top_k_dynamic_filtering=0)

# Print the optimized answer and assert it against the unoptimized one, with one process per
# setting rather than one per comparison.
compare() {
    local on off
    on=$("${LOCAL[@]}" "${ON[@]}" --query "$1" 2>&1)
    off=$("${LOCAL[@]}" "${OFF[@]}" --query "$1" 2>&1)
    echo "${on}"
    if [ "${on}" = "${off}" ]; then
        echo "OK"
    else
        echo "MISMATCH"
        diff <(echo "${on}") <(echo "${off}")
    fi
}

"${LOCAL[@]}" --query "
    INSERT INTO FUNCTION file('${DIR}/narrow1.parquet', Parquet)
    SELECT toInt32(1000 + number) AS x FROM numbers(1000)
    SETTINGS output_format_parquet_row_group_size = 1000, engine_file_truncate_on_insert = 1;

    INSERT INTO FUNCTION file('${DIR}/narrow2.parquet', Parquet)
    SELECT arrayJoin([toInt32(65541), toInt32(70000)]) AS x
    SETTINGS engine_file_truncate_on_insert = 1;

    INSERT INTO FUNCTION file('${DIR}/narrow3.parquet', Parquet)
    SELECT toInt32(2) AS x FROM numbers(1000)
    SETTINGS output_format_parquet_row_group_size = 1000, engine_file_truncate_on_insert = 1;

    INSERT INTO FUNCTION file('${DIR}/narrow4.parquet', Parquet)
    SELECT arrayJoin([toInt32(3), toInt32(65537)]) AS x
    SETTINGS engine_file_truncate_on_insert = 1;

    INSERT INTO FUNCTION file('${DIR}/uuid1.parquet', Parquet)
    SELECT '00000000-0000-0000-0000-00000000000a'::UUID AS u FROM numbers(1000)
    SETTINGS output_format_parquet_row_group_size = 1000, engine_file_truncate_on_insert = 1;

    INSERT INTO FUNCTION file('${DIR}/uuid2.parquet', Parquet)
    SELECT arrayJoin([
        '00000000-0000-0001-0000-000000000014'::UUID,
        '00000000-0000-0002-0000-000000000005'::UUID,
        '00000000-0000-0003-0000-00000000001e'::UUID]) AS u
    SETTINGS engine_file_truncate_on_insert = 1;

    INSERT INTO FUNCTION file('${DIR}/uuid3.parquet', Parquet)
    SELECT arrayJoin([
        '00000000-0000-0000-ffff-ffffffffffff'::UUID,
        'ffffffff-ffff-ffff-0000-000000000001'::UUID]) AS u
    SETTINGS engine_file_truncate_on_insert = 1;
"

echo "-- a narrowing type hint decodes the statistics and the values into different value spaces:"
echo "-- castColumn wraps the values around while the min/max Fields keep the raw physical value"
echo "-- (issue #118383), so the row group must not be skipped by a bound the output type cannot"
echo "-- even hold. narrow2 stores Int32 65541 and 70000, which read as UInt16 5 and 4464"
compare "SELECT x FROM file('${DIR}/narrow{1,2}.parquet', Parquet, 'x UInt16') ORDER BY x LIMIT 3"
compare "SELECT x FROM file('${DIR}/narrow{1,2}.parquet', Parquet, 'x UInt16') ORDER BY x DESC LIMIT 3"

echo "-- and when only one of the two bounds is unrepresentable the other one stops bounding the"
echo "-- row group as well: narrow4 stores Int32 3 and 65537, which read as UInt16 3 and 1, so its"
echo "-- decodable min of 3 would skip the very row group that holds the answer 1"
compare "SELECT x FROM file('${DIR}/narrow{3,4}.parquet', Parquet, 'x UInt16') ORDER BY x LIMIT 3"

echo "-- a UUID sort key: Parquet orders UUID statistics bytewise while ClickHouse compares the two"
echo "-- 64-bit halves in the opposite order (issue #118371), so the bytewise min/max neither bound"
echo "-- the row group nor even stay in order. uuid2 holds the true top-1 behind its bytewise min"
compare "SELECT u FROM file('${DIR}/uuid{1,2}.parquet', Parquet) ORDER BY u LIMIT 3"
compare "SELECT u FROM file('${DIR}/uuid{1,2}.parquet', Parquet) ORDER BY u DESC LIMIT 3"

echo "-- uuid3 is the same mismatch seen from the other side: its bytewise min is above its bytewise"
echo "-- max in ClickHouse order, which would be rejected as self-contradictory statistics"
compare "SELECT u FROM file('${DIR}/uuid3.parquet', Parquet) ORDER BY u LIMIT 2"
