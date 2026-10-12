#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: writes and reads Parquet files

# ORDER BY ... LIMIT with a single sort column over Parquet: a row group whose best value only equals
# the top-K threshold cannot improve the result, so the threshold skips it. With more sort columns
# such a row group can still tie-break into the result and is read.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR=$(mktemp -d "${CLICKHOUSE_TMP}/05333_parquet_topk_ties_XXXXXX")
trap 'rm -rf "${DIR}"' EXIT

LOCAL=(${CLICKHOUSE_LOCAL}
    --use_top_k_dynamic_filtering_for_variable_length_types=0
    --query_plan_max_limit_for_top_k_optimization=10000
    --input_format_parquet_use_native_reader_v3=1
    --input_format_parquet_filter_push_down=1
    --input_format_parquet_preserve_order=0
    --max_block_size=65409)
ST=(--max_threads=1 --max_parsing_threads=1)
MT=(--max_threads=8 --max_parsing_threads=8)
ON=(--use_top_k_dynamic_filtering=1)
OFF=(--use_top_k_dynamic_filtering=0)

# 20 row groups of 1000 rows. Each row group has five rows with k = 0 (its minimum) and five rows
# with k = 1000000 (its maximum); the other values of k are distinct. Row group 3 has a NULL in n.
DATA="SELECT multiIf(number % 1000 < 5, 0, number % 1000 < 10, 1000000, number + 1) AS k, number AS v,
    if(number = 3007, NULL, number % 1000) AS n FROM numbers(20000)"
"${LOCAL[@]}" --query "
    INSERT INTO FUNCTION file('${DIR}/t.parquet', Parquet) ${DATA}
    SETTINGS output_format_parquet_row_group_size = 1000, engine_file_truncate_on_insert = 1"
"${LOCAL[@]}" --query "SELECT num_row_groups FROM file('${DIR}/t.parquet', ParquetMetadata)"

# Every row group has five rows equal to its minimum (maximum), and they pass a non-strict threshold filter.
# So `rows_read` is 1000 for the first row group plus 5 for every other row group that is read: 1095 if all are.
run_json() {
    "${LOCAL[@]}" "${ST[@]}" "${ON[@]}" --query "$1" --format JSON | python3 -c "
import sys, json
d = json.load(sys.stdin)
print([list(r.values()) for r in d['data']], 'rows read < 1050:', d['statistics']['rows_read'] < 1050)"
}

# $1: arguments for the number of threads, $2: query
compare() {
    diff \
        <("${LOCAL[@]}" $1 "${ON[@]}" --query "$2") \
        <("${LOCAL[@]}" $1 "${OFF[@]}" --query "$2") \
        && echo "OK"
}

echo "-- one sort column: once the threshold reaches the shared minimum (maximum), the other row groups are skipped"
run_json "SELECT k FROM file('${DIR}/t.parquet') ORDER BY k LIMIT 3"
run_json "SELECT k FROM file('${DIR}/t.parquet') ORDER BY k DESC LIMIT 3"

echo "-- two sort columns: a row equal to the threshold can still win on v, so every row group is read"
run_json "SELECT k, v FROM file('${DIR}/t.parquet') ORDER BY k, v DESC LIMIT 3"

echo "-- results identical with and without the optimization"
queries=(
    "SELECT k FROM file('${DIR}/t.parquet') ORDER BY k LIMIT 7"
    "SELECT k FROM file('${DIR}/t.parquet') ORDER BY k DESC LIMIT 7"
    "SELECT k FROM file('${DIR}/t.parquet') ORDER BY k LIMIT 7 OFFSET 97"
    "SELECT k FROM file('${DIR}/t.parquet') WHERE v % 3 = 0 ORDER BY k LIMIT 7"
    "SELECT k FROM file('${DIR}/t.parquet') WHERE v % 1000 >= 5 ORDER BY k LIMIT 7"
    "SELECT k, v FROM file('${DIR}/t.parquet') ORDER BY k, v DESC LIMIT 7"
    "SELECT k, v FROM file('${DIR}/t.parquet') ORDER BY k DESC, v LIMIT 7"
    "SELECT count() FROM (SELECT k FROM file('${DIR}/t.parquet') ORDER BY k LIMIT 3 WITH TIES)"
    "SELECT n FROM file('${DIR}/t.parquet') ORDER BY n NULLS FIRST LIMIT 7"
    "SELECT n FROM file('${DIR}/t.parquet') ORDER BY n LIMIT 30"
    "SELECT countIf(k = 0), count() FROM (SELECT k, v FROM file('${DIR}/t.parquet') ORDER BY k LIMIT 7)"
)
for query in "${queries[@]}"; do
    compare "${ST[*]}" "${query}"
    compare "${MT[*]}" "${query}"
done
