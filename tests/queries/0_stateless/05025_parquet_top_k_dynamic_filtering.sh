#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: writes and reads Parquet files

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR=$(mktemp -d "${CLICKHOUSE_TMP}/05025_parquet_topk_XXXXXX")
trap 'rm -rf "${DIR}"' EXIT

# Pin everything the flaky check randomizes and everything the assertions depend on: the top-k
# settings, the native Parquet reader (the only one that consumes the top-k filter), its
# stats-based pruning, and single-threaded reading (the two-file pruning assertion below relies
# on the files being read one after another).
LOCAL=(${CLICKHOUSE_LOCAL}
    --use_top_k_dynamic_filtering_for_variable_length_types=0
    --query_plan_max_limit_for_top_k_optimization=1000
    --input_format_parquet_use_native_reader_v3=1
    --input_format_parquet_filter_push_down=1
    --max_block_size=65409
    --max_threads=1 --max_parsing_threads=1)
ON=(--use_top_k_dynamic_filtering=1)
OFF=(--use_top_k_dynamic_filtering=0)

# part1 is a single row group, so it can never be pruned as a whole: the `rows_read` bound below
# stays valid no matter how reading races with the sorting. part2 has 10 row groups, k strictly
# greater than everything in part1, and no nulls: once part1 has been read (files of a glob are
# read one after another in a single stream), the top-K threshold proves that no row group of
# part2 can contain a smaller k, and all of them are skipped without reading any column data.
"${LOCAL[@]}" --query "
    INSERT INTO FUNCTION file('${DIR}/part1.parquet', Parquet)
    SELECT number AS k, number * 10 AS v, if(number % 3 = 0, NULL, toInt64(number % 1000)) AS n
    FROM numbers(100000)
    SETTINGS output_format_parquet_row_group_size = 1000000, engine_file_truncate_on_insert = 1;

    INSERT INTO FUNCTION file('${DIR}/part2.parquet', Parquet)
    SELECT 100000 + number AS k, number * 10 AS v, toInt64(number % 1000) AS n
    FROM numbers(100000)
    SETTINGS output_format_parquet_row_group_size = 10000, engine_file_truncate_on_insert = 1;
"

run_json() {
    "${LOCAL[@]}" "$@" --format JSON | python3 -c "
import sys, json
d = json.load(sys.stdin)
print([list(r.values()) for r in d['data']], 'all of part2 skipped:', d['statistics']['rows_read'] <= 100000)"
}

echo "-- row-group pruning: part1 establishes the threshold before part2 starts, so every row"
echo "-- group of part2 is skipped and at most the 100000 rows of part1 are read"
run_json "${ON[@]}" --query "SELECT k FROM file('${DIR}/part{1,2}.parquet', Parquet) ORDER BY k LIMIT 3"

echo "-- without the optimization all 200000 rows are read"
run_json "${OFF[@]}" --query "SELECT k FROM file('${DIR}/part{1,2}.parquet', Parquet) ORDER BY k LIMIT 3"

echo "-- results identical with and without the optimization"
queries=(
    "SELECT k, v FROM file('${DIR}/part{1,2}.parquet', Parquet) ORDER BY k LIMIT 7"
    "SELECT k, v FROM file('${DIR}/part{1,2}.parquet', Parquet) ORDER BY k DESC LIMIT 7"
    "SELECT k, v FROM file('${DIR}/part{1,2}.parquet', Parquet) WHERE v % 40 = 0 ORDER BY k LIMIT 7"
    "SELECT k, v FROM file('${DIR}/part{1,2}.parquet', Parquet) ORDER BY k LIMIT 7 OFFSET 13"
    "SELECT k, n FROM file('${DIR}/part1.parquet', Parquet) ORDER BY n NULLS FIRST, k LIMIT 7"
    "SELECT k, n FROM file('${DIR}/part1.parquet', Parquet) ORDER BY n NULLS LAST, k LIMIT 7"
    "SELECT k, n FROM file('${DIR}/part1.parquet', Parquet) ORDER BY n DESC NULLS FIRST, k LIMIT 7"
    "SELECT k, v FROM file('${DIR}/part{1,2}.parquet', Parquet) ORDER BY k, v LIMIT 7"
    "SELECT k FROM file('${DIR}/part1.parquet', Parquet) WHERE k < 3 ORDER BY k LIMIT 100"
)
for query in "${queries[@]}"; do
    diff \
        <("${LOCAL[@]}" "${ON[@]}" --query "${query}") \
        <("${LOCAL[@]}" "${OFF[@]}" --query "${query}") \
        && echo "OK"
done

echo "-- collated ORDER BY: Parquet min/max statistics are bytewise, not collation-ordered, so the"
echo "-- row-group shortcut must stay off; 'ä' sorts before 'b' in the 'de' locale but its UTF-8"
echo "-- bytes are above 'z', so a stats-based skip of the second file would lose it"
"${LOCAL[@]}" --query "
    INSERT INTO FUNCTION file('${DIR}/coll1.parquet', Parquet)
    SELECT arrayJoin(['b', 'c', 'd']) AS s SETTINGS engine_file_truncate_on_insert = 1;

    INSERT INTO FUNCTION file('${DIR}/coll2.parquet', Parquet)
    SELECT arrayJoin(['z', 'ä']) AS s SETTINGS engine_file_truncate_on_insert = 1;
"
collate_query="SELECT s FROM file('${DIR}/coll{1,2}.parquet', Parquet) ORDER BY s COLLATE 'de' LIMIT 3"
# `LOCAL` pins the variable-length opt-in to 0; a String sort column needs it on.
LOCAL_STRING=("${LOCAL[@]/--use_top_k_dynamic_filtering_for_variable_length_types=0/--use_top_k_dynamic_filtering_for_variable_length_types=1}")
"${LOCAL_STRING[@]}" "${ON[@]}" --query "${collate_query}"
diff \
    <("${LOCAL_STRING[@]}" "${ON[@]}" --query "${collate_query}") \
    <("${LOCAL_STRING[@]}" "${OFF[@]}" --query "${collate_query}") \
    && echo "OK"
