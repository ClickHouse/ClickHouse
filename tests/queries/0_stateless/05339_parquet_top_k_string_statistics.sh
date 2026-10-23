#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: writes and reads Parquet files, needs S3 (MinIO) and Iceberg

# ORDER BY <String column> LIMIT over Parquet skips row groups by the statistics of the sort column
# also without `use_top_k_dynamic_filtering_for_variable_length_types`, but then without a per-row filter.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR=$(mktemp -d "${CLICKHOUSE_TMP}/05339_parquet_topk_XXXXXX")
trap 'rm -rf "${DIR}"' EXIT

LOCAL=(${CLICKHOUSE_LOCAL}
    --query_plan_max_limit_for_top_k_optimization=10000
    --input_format_parquet_filter_push_down=1
    --input_format_parquet_preserve_order=0
    --max_block_size=65409)
ST=(--max_threads=1 --max_parsing_threads=1)
MT=(--max_threads=8 --max_parsing_threads=8)
ON=(--use_top_k_dynamic_filtering=1 --use_top_k_dynamic_filtering_for_variable_length_types=0)
OPT_IN=(--use_top_k_dynamic_filtering=1 --use_top_k_dynamic_filtering_for_variable_length_types=1)
OFF=(--use_top_k_dynamic_filtering=0)

# 20 row groups of 1000 rows with disjoint ranges of k, not in the order of k: the smallest k are in row
# group 11, the largest in row group 8. `s` sorts as k; `d` has ties, also between row groups; `n` has a
# NULL in row groups 0, 5, 10 and 15; `e` is '' in every tenth row, so all row groups share the minimum.
DATA="SELECT leftPad(toString(k), 6, '0') AS s, number AS v, leftPad(toString(intDiv(k + 5, 10)), 5, '0') AS d,
        if(number % 5000 = 7, NULL, s) AS n, if(number % 10 = 0, '', s) AS e
    FROM (SELECT (number % 1000) + 1000 * ((intDiv(number, 1000) * 7 + 3) % 20) AS k, number FROM numbers(20000))"
"${LOCAL[@]}" --query "
    INSERT INTO FUNCTION file('${DIR}/t.parquet', Parquet) ${DATA}
    SETTINGS output_format_parquet_row_group_size = 1000, engine_file_truncate_on_insert = 1"
"${LOCAL[@]}" --query "SELECT num_row_groups FROM file('${DIR}/t.parquet', ParquetMetadata)"

# An Int64 column read as String: the string order differs from the numeric order of the statistics.
"${LOCAL[@]}" --query "
    INSERT INTO FUNCTION file('${DIR}/i.parquet', Parquet)
    SELECT toInt64(multiIf(number < 10, 100 + number, number < 20, number - 5, number)) AS x, number AS y FROM numbers(30)
    SETTINGS output_format_parquet_row_group_size = 10, engine_file_truncate_on_insert = 1"

# FixedString(4) is written as FIXED_LEN_BYTE_ARRAY, whose statistics keep the trailing zero bytes that a
# read as String strips. Row group 0 has f = 'b', row groups 1 to 5 f = 'ab'; only the last one has v < 1000.
"${LOCAL[@]}" --query "
    INSERT INTO FUNCTION file('${DIR}/f.parquet', Parquet)
    SELECT toFixedString(if(number < 1000, 'b', 'ab'), 4) AS f, if(number < 5000, 1000 + number % 1000, number % 1000) AS v
    FROM numbers(6000)
    SETTINGS output_format_parquet_row_group_size = 1000, max_threads = 1, max_insert_threads = 1, engine_file_truncate_on_insert = 1"

run_json() {
    # $1: query, $2: Python condition on `rows_read`
    "${LOCAL[@]}" "${ST[@]}" "${ON[@]}" --query "$1" --format JSON | python3 -c "
import sys, json
d = json.load(sys.stdin)
rows_read = d['statistics']['rows_read']
print([list(r.values()) for r in d['data']], '$2:', $2)"
}

# Runs all of ${queries} in one process, each result followed by a line '---'. $@: settings.
run_queries() {
    local script="" query
    for query in "${queries[@]}"; do
        script+="${query}; SELECT '---'; "
    done
    "${LOCAL[@]}" "$@" --query "${script}"
}

# $@: arguments for the number of threads
compare() {
    python3 -c "
import sys
def results(output):
    groups, lines = [], []
    for line in output.split('\n'):
        if line == '---':
            groups.append(lines)
            lines = []
        else:
            lines.append(line)
    return groups
off, on, opt_in = (results(output) for output in sys.argv[1:4])
for i, query in enumerate(sys.argv[4:]):
    same = i < len(off) and off[i] and i < len(on) and on[i] == off[i] and i < len(opt_in) and opt_in[i] == off[i]
    print('OK' if same else 'FAIL: ' + query)
" "$(run_queries "$@" "${OFF[@]}")" "$(run_queries "$@" "${ON[@]}")" "$(run_queries "$@" "${OPT_IN[@]}")" "${queries[@]}"
}

echo "-- the row group with the smallest (largest) value is read first and the others are skipped"
run_json "SELECT s FROM file('${DIR}/t.parquet') ORDER BY s LIMIT 3" "rows_read <= 2000"
run_json "SELECT s FROM file('${DIR}/t.parquet') ORDER BY s DESC LIMIT 3" "rows_read <= 2000"
run_json "SELECT s FROM file('${DIR}/t.parquet') WHERE v % 2 = 0 ORDER BY s DESC LIMIT 3" "rows_read <= 2000"
run_json "SELECT n FROM file('${DIR}/t.parquet') ORDER BY n DESC LIMIT 3" "rows_read <= 6000"
run_json "SELECT s FROM file('${DIR}/t.parquet', Parquet, 's LowCardinality(String)') ORDER BY s LIMIT 3" "rows_read <= 2000"
run_json "SELECT n FROM file('${DIR}/t.parquet', Parquet, 'n LowCardinality(Nullable(String))') ORDER BY n LIMIT 3" "rows_read <= 6000"

echo "-- not for a LIMIT at which the sorting filters rows by its own threshold"
run_json "SELECT count() FROM (SELECT s FROM file('${DIR}/t.parquet') ORDER BY s LIMIT 1500)" "rows_read == 20000"

echo "-- results identical with and without the optimization"
queries=(
    "SELECT s, v FROM file('${DIR}/t.parquet') ORDER BY s LIMIT 7"
    "SELECT s, v FROM file('${DIR}/t.parquet') ORDER BY s DESC LIMIT 7"
    "SELECT s, v FROM file('${DIR}/t.parquet') WHERE v % 3 = 0 ORDER BY s LIMIT 7"
    "SELECT s, v FROM file('${DIR}/t.parquet') ORDER BY s LIMIT 7 OFFSET 2500"
    "SELECT d, v FROM file('${DIR}/t.parquet') ORDER BY d, v DESC LIMIT 7"
    "SELECT d, v FROM file('${DIR}/t.parquet') ORDER BY d DESC, v LIMIT 7"
    "SELECT e, v FROM file('${DIR}/t.parquet') WHERE e != '' ORDER BY e, v LIMIT 7"
    "SELECT e, v FROM file('${DIR}/t.parquet') ORDER BY e DESC, v LIMIT 7"
    "SELECT n, v FROM file('${DIR}/t.parquet') ORDER BY n NULLS FIRST, v LIMIT 7"
    "SELECT n, v FROM file('${DIR}/t.parquet') ORDER BY n NULLS LAST, v LIMIT 7"
    "SELECT n, v FROM file('${DIR}/t.parquet') ORDER BY n DESC NULLS FIRST, v LIMIT 7"
    "SELECT n, v FROM file('${DIR}/t.parquet') ORDER BY n DESC NULLS LAST, v LIMIT 7"
    "SELECT s, v FROM file('${DIR}/t.parquet') ORDER BY s DESC COLLATE 'en', v LIMIT 7"
    "SELECT s, v FROM file('${DIR}/t.parquet', Parquet, 's LowCardinality(String), v UInt64') ORDER BY s LIMIT 7"
    "SELECT s, v FROM file('${DIR}/t.parquet') ORDER BY s LIMIT 7 SETTINGS input_format_parquet_preserve_order = 1"
    "SELECT s, v FROM file('${DIR}/t.parquet') PREWHERE s LIKE '%7%' ORDER BY s DESC LIMIT 7 SETTINGS apply_string_filters_during_scan = 1"
    "SELECT s, v FROM file('${DIR}/t.parquet') ORDER BY s LIMIT 1500"
    "SELECT x, y FROM file('${DIR}/i.parquet', Parquet, 'x String, y UInt64') ORDER BY x, y LIMIT 3"
    "SELECT x, y FROM file('${DIR}/i.parquet', Parquet, 'x String, y UInt64') ORDER BY x DESC, y LIMIT 3"
    "SELECT f, v FROM file('${DIR}/f.parquet', Parquet, 'f String, v UInt64') ORDER BY f, v LIMIT 3"
)
compare "${ST[@]}"
compare "${MT[@]}"

echo "-- a Memory table gets the per-row filter only with the opt-in"
for opt_in in 0 1; do
    "${LOCAL[@]}" --use_top_k_dynamic_filtering=1 --use_top_k_dynamic_filtering_for_variable_length_types=${opt_in} --query "
        CREATE TABLE t_mem (s String) ENGINE = Memory;
        INSERT INTO t_mem SELECT toString(number) FROM numbers(10);
        SELECT count() FROM (EXPLAIN actions = 1 SELECT s FROM t_mem ORDER BY s LIMIT 3) WHERE explain LIKE '%TopN filter%'"
done

# A table under `user_files` with an mtime in the past, so that the query condition cache trusts the file version.
${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS t_05339;
    CREATE TABLE t_05339 (s String, v UInt64) ENGINE = File(Parquet, '${CLICKHOUSE_DATABASE}/05339.parquet')
        SETTINGS output_format_parquet_row_group_size = 1000, input_format_parquet_filter_push_down = 1,
            input_format_parquet_preserve_order = 0;
    INSERT INTO t_05339 SELECT s, v FROM (${DATA})
        SETTINGS output_format_parallel_formatting = 0, max_threads = 1, max_insert_threads = 1, max_block_size = 65409;
"
touch -d '2020-01-01 00:00:00' "${USER_FILES_PATH:?}/${CLICKHOUSE_DATABASE}/05339.parquet"
${CLICKHOUSE_CLIENT} --query "SELECT num_row_groups FROM file('${CLICKHOUSE_DATABASE}/05339.parquet', ParquetMetadata)"

S3_FILE="${CLICKHOUSE_DATABASE}_05339/t.parquet"
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION s3(s3_conn, filename = '${S3_FILE}', format = Parquet) SELECT s, v FROM (${DATA})
        SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 1000, output_format_parallel_formatting = 0,
            max_threads = 1, max_insert_threads = 1, max_block_size = 65409;
    DROP TABLE IF EXISTS t_05339_s3;
    CREATE TABLE t_05339_s3 (s String, v UInt64) ENGINE = S3(s3_conn, filename = '${S3_FILE}', format = Parquet);
    SELECT num_row_groups FROM s3(s3_conn, filename = '${S3_FILE}', format = ParquetMetadata);
"

SETTINGS="use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 10000, max_threads = 1,
    max_parsing_threads = 1, max_block_size = 65409, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1,
    input_format_parquet_filter_push_down = 1, input_format_parquet_preserve_order = 0"
query_id="05339_${CLICKHOUSE_DATABASE}_${RANDOM}"

echo "-- the skipped row groups are counted, and no rows are filtered by the threshold"
for arm in "asc 0" "desc 0" "asc 1"; do
    read -r direction opt_in <<< "${arm}"
    ${CLICKHOUSE_CLIENT} --query_id "${query_id}_skip_${direction}_${opt_in}" --query "
        SELECT s FROM t_05339 ORDER BY s ${direction} LIMIT 3
        SETTINGS ${SETTINGS}, use_query_condition_cache = 0, use_top_k_dynamic_filtering_for_variable_length_types = ${opt_in}" > /dev/null
done
${CLICKHOUSE_CLIENT} --query "
    SYSTEM FLUSH LOGS query_log;
    SELECT replaceOne(query_id, '${query_id}_skip_', ''),
        ProfileEvents['ParquetTopKSkippedRowGroups'] BETWEEN 17 AND 19,
        ProfileEvents['ParquetRowsFilterExpression'] = 0
    FROM system.query_log
    WHERE current_database = currentDatabase() AND startsWith(query_id, '${query_id}_skip_') AND type = 'QueryFinish'
    ORDER BY query_id;
"

echo "-- for the query condition cache such a read is a plain read"
# The predicate is false only in row group 8, which holds the largest s, so the read for DESC skips row groups
# whose rows match it. Another test can drop the cache at any time, hence the attempts, each with a new predicate.
qcc_settings="${SETTINGS}, use_top_k_dynamic_filtering_for_variable_length_types = 0"
check_query_condition_cache() {
    # $1: table
    local correct=1 predicate with_cache without_cache
    for attempt in 1 2 3; do
        predicate="intDiv(v, 1000) != 8 AND v != ${attempt}000000"
        ${CLICKHOUSE_CLIENT} --query "
            SELECT s FROM $1 WHERE ${predicate} ORDER BY s DESC LIMIT 3 SETTINGS ${qcc_settings}, use_query_condition_cache = 1" > /dev/null
        with_cache=$(${CLICKHOUSE_CLIENT} --query_id "${query_id}_$1_count_${attempt}" --query "
            SELECT count() FROM $1 WHERE ${predicate} SETTINGS ${qcc_settings}, use_query_condition_cache = 1")
        without_cache=$(${CLICKHOUSE_CLIENT} --query "
            SELECT count() FROM $1 WHERE ${predicate} SETTINGS ${qcc_settings}, use_query_condition_cache = 0")
        [ "${with_cache}" = "${without_cache}" ] || correct=0
        ${CLICKHOUSE_CLIENT} --query_id "${query_id}_$1_topk_${attempt}" --query "
            SELECT s FROM $1 WHERE ${predicate} ORDER BY s DESC LIMIT 3
            SETTINGS ${qcc_settings}, use_query_condition_cache = 1, use_query_condition_cache_for_top_k = 0" > /dev/null
    done
    echo "$1: count without the cache: ${without_cache}"
    echo "$1: count with the cache is the count without it: ${correct}"
    ${CLICKHOUSE_CLIENT} --query "
        SYSTEM FLUSH LOGS query_log;
        SELECT '$1: the count is read from the cache:', max(ProfileEvents['QueryConditionCacheHits'] > 0)
        FROM system.query_log
        WHERE current_database = currentDatabase() AND startsWith(query_id, '${query_id}_$1_count_') AND type = 'QueryFinish';
        SELECT '$1: the read consults the cache without use_query_condition_cache_for_top_k:', max(ProfileEvents['QueryConditionCacheHits'] > 0)
        FROM system.query_log
        WHERE current_database = currentDatabase() AND startsWith(query_id, '${query_id}_$1_topk_') AND type = 'QueryFinish';
    "
}
check_query_condition_cache t_05339
check_query_condition_cache t_05339_s3

check_query_condition_cache_written() {
    # $1: table
    local predicate
    for attempt in 1 2 3; do
        predicate="intDiv(v, 1000) != 8 AND v != ${attempt}100000"
        ${CLICKHOUSE_CLIENT} --query "
            SELECT s FROM $1 WHERE ${predicate} ORDER BY s DESC LIMIT 3
            SETTINGS ${qcc_settings}, use_query_condition_cache = 1, use_query_condition_cache_for_top_k = 0" > /dev/null
        ${CLICKHOUSE_CLIENT} --query_id "${query_id}_$1_written_${attempt}" --query "
            SELECT count() FROM $1 WHERE ${predicate} SETTINGS ${qcc_settings}, use_query_condition_cache = 1" > /dev/null
    done
    ${CLICKHOUSE_CLIENT} --query "
        SYSTEM FLUSH LOGS query_log;
        SELECT '$1: the read populates the cache without use_query_condition_cache_for_top_k:', max(ProfileEvents['QueryConditionCacheHits'] > 0)
        FROM system.query_log
        WHERE current_database = currentDatabase() AND startsWith(query_id, '${query_id}_$1_written_') AND type = 'QueryFinish';
    "
}
check_query_condition_cache_written t_05339
check_query_condition_cache_written t_05339_s3

# Unlike a TopK read of a `File` table with the per-row filter, it does not use the cache without a filter condition.
${CLICKHOUSE_CLIENT} --query_id "${query_id}_no_filter" --query "
    SELECT s FROM t_05339 ORDER BY s DESC LIMIT 3 SETTINGS ${qcc_settings}, use_query_condition_cache = 1" > /dev/null
${CLICKHOUSE_CLIENT} --query "
    SYSTEM FLUSH LOGS query_log;
    SELECT 't_05339: a read without a filter does not use the cache:',
        ProfileEvents['QueryConditionCacheHits'] + ProfileEvents['QueryConditionCacheMisses'] = 0
    FROM system.query_log
    WHERE current_database = currentDatabase() AND query_id = '${query_id}_no_filter' AND type = 'QueryFinish';
"

# Nor does a read of a data lake table, which uses the cache without a filter condition with the per-row filter.
ICEBERG_DIR="${USER_FILES_PATH}/lakehouses/${CLICKHOUSE_DATABASE}_05339"
rm -rf "${ICEBERG_DIR}"
${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS t_05339_iceberg;
    CREATE TABLE t_05339_iceberg (s String, v Int64) ENGINE = IcebergLocal('${ICEBERG_DIR}', 'Parquet')
        SETTINGS output_format_parquet_row_group_size = 1000, output_format_parallel_formatting = 0"
${CLICKHOUSE_CLIENT} --allow_experimental_insert_into_iceberg 1 --query "
    INSERT INTO t_05339_iceberg SELECT s, toInt64(v) FROM (${DATA}) SETTINGS max_threads = 1, max_insert_threads = 1, max_block_size = 65409"
for opt_in in 0 1; do
    ${CLICKHOUSE_CLIENT} --query_id "${query_id}_iceberg_${opt_in}" --query "
        SELECT s FROM t_05339_iceberg ORDER BY s DESC LIMIT 3
        SETTINGS ${SETTINGS}, use_query_condition_cache = 1, use_query_condition_cache_for_top_k = 1, enable_parallel_replicas = 0,
            use_top_k_dynamic_filtering_for_variable_length_types = ${opt_in}" > /dev/null
done
${CLICKHOUSE_CLIENT} --query "
    SYSTEM FLUSH LOGS query_log;
    SELECT 't_05339_iceberg: the read uses the cache, with the opt-in ' || splitByChar('_', query_id)[-1] || ':',
        ProfileEvents['QueryConditionCacheHits'] + ProfileEvents['QueryConditionCacheMisses'] > 0
    FROM system.query_log
    WHERE current_database = currentDatabase() AND startsWith(query_id, '${query_id}_iceberg_') AND type = 'QueryFinish'
    ORDER BY query_id;
"

echo "-- in object storage, also for row groups skipped after the read of their file has started"
# Two objects, read in key order. In each one every row group holds the same values, so its row groups are not ordered
# and are checked against the threshold only once they were started. All values of a.parquet are larger than those
# of b.parquet, so every row group of b.parquet is skipped.
S3_GLOB="${CLICKHOUSE_DATABASE}_05339_glob"
for part in "a c" "b b"; do
    read -r name prefix <<< "${part}"
    ${CLICKHOUSE_CLIENT} --query "
        INSERT INTO FUNCTION s3(s3_conn, filename = '${S3_GLOB}/${name}.parquet', format = Parquet)
        SELECT concat('${prefix}', leftPad(toString(number % 100), 3, '0')) AS s, number AS v FROM numbers(1000)
        SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 100, output_format_parallel_formatting = 0,
            max_threads = 1, max_insert_threads = 1, max_block_size = 65409"
done
${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS t_05339_s3_glob;
    CREATE TABLE t_05339_s3_glob (s String, v UInt64) ENGINE = S3(s3_conn, filename = '${S3_GLOB}/*.parquet', format = Parquet);
"
${CLICKHOUSE_CLIENT} --query_id "${query_id}_glob" --query "
    SELECT s FROM t_05339_s3_glob ORDER BY s DESC LIMIT 3
    SETTINGS ${SETTINGS}, use_query_condition_cache = 0, use_top_k_dynamic_filtering_for_variable_length_types = 0" > /dev/null
${CLICKHOUSE_CLIENT} --query "
    SYSTEM FLUSH LOGS query_log;
    SELECT ProfileEvents['ParquetTopKSkippedRowGroups'], ProfileEvents['ParquetRowsFilterExpression']
    FROM system.query_log
    WHERE current_database = currentDatabase() AND query_id = '${query_id}_glob' AND type = 'QueryFinish';
"
check_query_condition_cache t_05339_s3_glob

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05339; DROP TABLE t_05339_s3; DROP TABLE t_05339_s3_glob; DROP TABLE t_05339_iceberg"
rm -f "${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}/05339.parquet"
rm -rf "${ICEBERG_DIR}"
