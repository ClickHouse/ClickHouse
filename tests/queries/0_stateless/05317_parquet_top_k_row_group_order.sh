#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: writes and reads Parquet files

# ORDER BY ... LIMIT over a single Parquet file reads first the row groups whose statistics are the
# best for the top-K threshold, so the threshold skips the other row groups.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR=$(mktemp -d "${CLICKHOUSE_TMP}/05317_parquet_topk_XXXXXX")
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

# 20 row groups of 1000 rows with disjoint ranges of k, not in the order of k: the smallest k are in
# row group 11, the largest in row group 8. Row groups 0, 5, 10 and 15 have a NULL in n.
DATA="SELECT (number % 1000) + 1000 * ((intDiv(number, 1000) * 7 + 3) % 20) AS k, number AS v, if(number % 5000 = 7, NULL, number) AS n
    FROM numbers(20000)"
"${LOCAL[@]}" --query "
    INSERT INTO FUNCTION file('${DIR}/t.parquet', Parquet) ${DATA}
    SETTINGS output_format_parquet_row_group_size = 1000, engine_file_truncate_on_insert = 1"
"${LOCAL[@]}" --query "SELECT num_row_groups FROM file('${DIR}/t.parquet', ParquetMetadata)"

run_json() {
    "${LOCAL[@]}" "${ST[@]}" "${ON[@]}" --query "$1" --format JSON | python3 -c "
import sys, json
d = json.load(sys.stdin)
print([list(r.values()) for r in d['data']], 'rows read <= 1000:', d['statistics']['rows_read'] <= 1000)"
}

# $1: arguments for the number of threads, $2: query
compare() {
    diff \
        <("${LOCAL[@]}" $1 "${ON[@]}" --query "$2") \
        <("${LOCAL[@]}" $1 "${OFF[@]}" --query "$2") \
        && echo "OK"
}

echo "-- the row group with the smallest (largest) k is read first and the others are skipped"
run_json "SELECT k FROM file('${DIR}/t.parquet') ORDER BY k LIMIT 3"
run_json "SELECT k FROM file('${DIR}/t.parquet') ORDER BY k DESC LIMIT 3"

echo "-- with input_format_parquet_preserve_order the row groups are read in file order, so none is skipped"
run_json "SELECT k FROM file('${DIR}/t.parquet') ORDER BY k LIMIT 3 SETTINGS input_format_parquet_preserve_order = 1"

echo "-- results identical with and without the optimization"
queries=(
    "SELECT k, v FROM file('${DIR}/t.parquet') ORDER BY k LIMIT 7"
    "SELECT k, v FROM file('${DIR}/t.parquet') ORDER BY k DESC LIMIT 7"
    "SELECT k, v FROM file('${DIR}/t.parquet') ORDER BY k LIMIT 7 OFFSET 2500"
    "SELECT k, v FROM file('${DIR}/t.parquet') WHERE v % 3 = 0 ORDER BY k LIMIT 7"
    "SELECT k, v FROM file('${DIR}/t.parquet') ORDER BY k, v LIMIT 7"
    "SELECT k, n FROM file('${DIR}/t.parquet') ORDER BY n NULLS FIRST, k LIMIT 7"
    "SELECT k, n FROM file('${DIR}/t.parquet') ORDER BY n DESC NULLS FIRST, k LIMIT 7"
    "SELECT k, v FROM file('${DIR}/t.parquet') ORDER BY k LIMIT 7 SETTINGS input_format_parquet_preserve_order = 1"
)
for query in "${queries[@]}"; do
    compare "${ST[*]}" "${query}"
    compare "${MT[*]}" "${query}"
done

echo "-- functions that depend on the rows of their block below the sort see the same rows"
block_queries=(
    "SELECT k, rowNumberInAllBlocks() AS r FROM file('${DIR}/t.parquet') ORDER BY k, r LIMIT 3"
    "SELECT k FROM file('${DIR}/t.parquet') WHERE rowNumberInAllBlocks() % 3 = 0 ORDER BY k LIMIT 3"
)
for query in "${block_queries[@]}"; do
    "${LOCAL[@]}" "${ST[@]}" "${ON[@]}" --query "${query}"
    compare "${ST[*]}" "${query}"
done

echo "-- the same for a row policy"
${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS t_05317;
    CREATE TABLE t_05317 (k UInt64, v UInt64, n Nullable(UInt64)) ENGINE = File(Parquet)
        SETTINGS output_format_parquet_row_group_size = 1000, input_format_parquet_use_native_reader_v3 = 1,
            input_format_parquet_filter_push_down = 1, input_format_parquet_preserve_order = 0;
    INSERT INTO t_05317 ${DATA} SETTINGS output_format_parallel_formatting = 0, max_threads = 1, max_insert_threads = 1, max_block_size = 65409;
    CREATE ROW POLICY p_05317_${CLICKHOUSE_DATABASE} ON t_05317 USING rowNumberInAllBlocks() % 3 = 0 TO ALL;
"
policy_query="SELECT k FROM t_05317 ORDER BY k LIMIT 3 SETTINGS max_threads = 1, max_parsing_threads = 1, max_block_size = 65409,
    query_plan_max_limit_for_top_k_optimization = 10000, use_top_k_dynamic_filtering ="
query_id="05317_${CLICKHOUSE_DATABASE}_${RANDOM}"
${CLICKHOUSE_CLIENT} --query_id "${query_id}" --query "${policy_query} 1"
diff \
    <(${CLICKHOUSE_CLIENT} --query "${policy_query} 1") \
    <(${CLICKHOUSE_CLIENT} --query "${policy_query} 0") \
    && echo "OK"
${CLICKHOUSE_CLIENT} --query "
    SYSTEM FLUSH LOGS query_log;
    SELECT 'row groups in the table:', ProfileEvents['ParquetReadRowGroups'] FROM system.query_log
    WHERE current_database = currentDatabase() AND query_id = '${query_id}' AND type = 'QueryFinish';
"
${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY p_05317_${CLICKHOUSE_DATABASE} ON t_05317"

echo "-- the row groups skipped by the threshold are counted"
skipped_query() {
    # $1: sort direction, $2: use_top_k_dynamic_filtering
    echo "SELECT k FROM t_05317 ORDER BY k $1 LIMIT 3 SETTINGS max_threads = 1, max_parsing_threads = 1,
        max_block_size = 65409, query_plan_max_limit_for_top_k_optimization = 10000, use_top_k_dynamic_filtering = $2"
}
skipped_id="05317_skipped_${CLICKHOUSE_DATABASE}_${RANDOM}"
${CLICKHOUSE_CLIENT} --query_id "${skipped_id}_asc" --query "$(skipped_query ASC 1)" > /dev/null
${CLICKHOUSE_CLIENT} --query_id "${skipped_id}_desc" --query "$(skipped_query DESC 1)" > /dev/null
${CLICKHOUSE_CLIENT} --query_id "${skipped_id}_off" --query "$(skipped_query ASC 0)" > /dev/null
${CLICKHOUSE_CLIENT} --query "
    SYSTEM FLUSH LOGS query_log;
    SELECT replaceOne(query_id, '${skipped_id}_', ''),
        ProfileEvents['ParquetTopKSkippedRowGroups'] BETWEEN 17 AND 20,
        ProfileEvents['ParquetTopKSkippedRowGroups'] = 0
    FROM system.query_log
    WHERE current_database = currentDatabase() AND startsWith(query_id, '${skipped_id}_') AND type = 'QueryFinish'
    ORDER BY query_id;
"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05317"
