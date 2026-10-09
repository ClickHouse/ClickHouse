#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: writes and reads Parquet files

# ORDER BY ... LIMIT over Parquet: a row group admitted before the top-K threshold excluded it is
# skipped when its column data is scheduled, if that data waits for a decoding thread meanwhile.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR="${CLICKHOUSE_DATABASE}_05346"
mkdir -p "${USER_FILES_PATH:?}/${DATA_DIR}"
rm -f "${USER_FILES_PATH}/${DATA_DIR}"/*.parquet

# 40 row groups of 2000 rows with disjoint ranges of k, not in file order; p = 1 in the second half
# of every row group.
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${DATA_DIR}/t.parquet', Parquet)
    SELECT k, number AS v, toUInt8(pos >= 1000) AS p, leftPad(toString(k), 6, '0') AS s
    FROM (SELECT number, number % 2000 AS pos, ((intDiv(number, 2000) * 7 + 3) % 40) * 2000 + pos AS k FROM numbers(80000))
    SETTINGS output_format_parquet_row_group_size = 2000, max_block_size = 2000, engine_file_truncate_on_insert = 1"

# Backdate the file so its version token has settled and the query condition cache engages.
touch -d '2020-01-01 00:00:00' "${USER_FILES_PATH}/${DATA_DIR}/t.parquet"

FILE="file('${DATA_DIR}/t.parquet', Parquet, 'k UInt64, v UInt64, p UInt8')"
FORMAT_SETTINGS="input_format_parquet_use_native_reader_v3 = 1, input_format_parquet_filter_push_down = 1,
    input_format_parquet_preserve_order = 0, input_format_parquet_memory_low_watermark = 0"
COMMON="query_plan_max_limit_for_top_k_optimization = 1000, ${FORMAT_SETTINGS}"

echo "-- results identical with and without the optimization"
queries=(
    "SELECT k, v FROM ${FILE} ORDER BY k LIMIT 3"
    "SELECT k, v FROM ${FILE} ORDER BY k DESC LIMIT 3"
    "SELECT k, v FROM ${FILE} WHERE p = 1 ORDER BY k LIMIT 3"
)
for threads in "max_threads = 1, max_parsing_threads = 2" "max_threads = 4, max_parsing_threads = 8"; do
    for block in 100 4000; do
        for prewhere in 1 0; do
            cfg="${threads}, input_format_parquet_max_block_size = ${block},
                optimize_move_to_prewhere = ${prewhere}, query_plan_optimize_prewhere = ${prewhere},
                use_query_condition_cache = 0, ${COMMON}"
            ok=1
            for query in "${queries[@]}"; do
                diff -q \
                    <(${CLICKHOUSE_CLIENT} --query "${query} SETTINGS ${cfg}, use_top_k_dynamic_filtering = 1") \
                    <(${CLICKHOUSE_CLIENT} --query "${query} SETTINGS ${cfg}, use_top_k_dynamic_filtering = 0") \
                    > /dev/null || { ok=0; echo "mismatch: ${query} SETTINGS ${cfg}"; }
            done
            [ "${ok}" = 1 ] && echo "threads (${threads}), block ${block}, prewhere ${prewhere}: OK"
        done
    done
done

echo "-- row groups excluded while their data waits are not read"
# 2 parsing threads admit 3 row groups before there is a threshold, and the decoding stage then runs one
# batch at a time, so the 2nd and 3rd ones wait behind the 20 row subgroups of the 1st one. Without the
# late check at most 40 - 3 = 37 row groups are skipped.
for i in $(seq 1 20); do
    ${CLICKHOUSE_CLIENT} --query_id "${CLICKHOUSE_TEST_UNIQUE_NAME}_late_${i}" --query "
        SELECT k FROM ${FILE} ORDER BY k LIMIT 3
        SETTINGS max_threads = 1, max_parsing_threads = 2, input_format_parquet_max_block_size = 100,
            use_top_k_dynamic_filtering = 1, use_query_condition_cache = 0, ${COMMON}" > /dev/null
done
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT max(ProfileEvents['ParquetTopKSkippedRowGroups']) >= 38
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish'
        AND startsWith(query_id, '${CLICKHOUSE_TEST_UNIQUE_NAME}_late_')"

echo "-- a String key: row groups excluded while their data waits are not read"
# The same late skip with the per-row filter enabled for a variable-length sort column.
for i in $(seq 1 20); do
    ${CLICKHOUSE_CLIENT} --query_id "${CLICKHOUSE_TEST_UNIQUE_NAME}_late_s_${i}" --query "
        SELECT s FROM file('${DATA_DIR}/t.parquet', Parquet, 's String') ORDER BY s LIMIT 3
        SETTINGS max_threads = 1, max_parsing_threads = 2, input_format_parquet_max_block_size = 100,
            use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_variable_length_types = 1,
            use_query_condition_cache = 0, ${COMMON}" > /dev/null
done
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT max(ProfileEvents['ParquetTopKSkippedRowGroups']) >= 38
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish'
        AND startsWith(query_id, '${CLICKHOUSE_TEST_UNIQUE_NAME}_late_s_')"

echo "-- the query condition cache after a read that skipped row groups late"
# A File table takes the format settings from its definition, not from the query.
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_05346"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_05346 (k UInt64, v UInt64, p UInt8, s String) ENGINE = File(Parquet, '${DATA_DIR}/t.parquet')
    SETTINGS input_format_parquet_max_block_size = 100, ${FORMAT_SETTINGS}"
CACHE="max_threads = 1, max_parsing_threads = 2, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1,
    use_query_condition_cache_for_top_k = 1, query_plan_max_limit_for_top_k_optimization = 1000"
# Each attempt has its own predicate, so its own cache entry: the second read must use the entry the first one wrote.
equal=1
for a in 1 2 3 4 5; do
    query="SELECT k, v FROM t_05346 WHERE p = 1 AND v != ${a}000000 ORDER BY k LIMIT 3"
    reference=$(${CLICKHOUSE_CLIENT} --query "${query} SETTINGS use_top_k_dynamic_filtering = 0, use_query_condition_cache = 0, ${CACHE}")
    for run in w r; do
        result=$(${CLICKHOUSE_CLIENT} --query_id "${CLICKHOUSE_TEST_UNIQUE_NAME}_cache_${run}_${a}" --query "${query}
            SETTINGS use_top_k_dynamic_filtering = 1, use_query_condition_cache = 1, ${CACHE}")
        [ "${result}" = "${reference}" ] || { equal=0; echo "mismatch: ${query}, run ${run}"; }
    done
done
echo "results equal: ${equal}"
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT max(w.skipped >= 38 AND r.hits > 0)
    FROM
    (
        SELECT extract(query_id, '_([0-9]+)$') AS a, ProfileEvents['ParquetTopKSkippedRowGroups'] AS skipped
        FROM system.query_log
        WHERE current_database = currentDatabase() AND type = 'QueryFinish'
            AND startsWith(query_id, '${CLICKHOUSE_TEST_UNIQUE_NAME}_cache_w_')
    ) AS w
    INNER JOIN
    (
        SELECT extract(query_id, '_([0-9]+)$') AS a, ProfileEvents['QueryConditionCacheHits'] AS hits
        FROM system.query_log
        WHERE current_database = currentDatabase() AND type = 'QueryFinish'
            AND startsWith(query_id, '${CLICKHOUSE_TEST_UNIQUE_NAME}_cache_r_')
    ) AS r USING (a)"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t_05346 WHERE p = 1 SETTINGS use_query_condition_cache = 1, ${CACHE}"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t_05346 WHERE p = 1 SETTINGS use_query_condition_cache = 0, ${CACHE}"

echo "-- a String key at the default settings: the query condition cache after the reads"
# A read that skips row groups must not leave a cache entry that hides rows from a later read with the same predicate.
for i in $(seq 1 20); do
    ${CLICKHOUSE_CLIENT} --query "SELECT s FROM t_05346 WHERE p = 1 AND v != 99999999 ORDER BY s LIMIT 3
        SETTINGS max_threads = 1, max_parsing_threads = 2, use_top_k_dynamic_filtering = 1,
            use_top_k_dynamic_filtering_for_variable_length_types = 0, use_query_condition_cache = 1,
            optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1,
            query_plan_max_limit_for_top_k_optimization = 1000" > /dev/null
done
for cache in 1 0; do
    ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t_05346 WHERE p = 1 AND v != 99999999
        SETTINGS use_query_condition_cache = ${cache}, max_threads = 1, max_parsing_threads = 2"
done

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05346"
rm -rf "${USER_FILES_PATH:?}/${DATA_DIR}"
