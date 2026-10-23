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

# Each clickhouse-local and clickhouse-client call runs a whole group of queries: starting the process is the slow part.
LOCAL=(${CLICKHOUSE_LOCAL}
    --query_plan_max_limit_for_top_k_optimization=10000
    --input_format_parquet_filter_push_down=1
    --input_format_parquet_preserve_order=0
    --max_block_size=65409)
ST="max_threads = 1, max_parsing_threads = 1"
MT="max_threads = 8, max_parsing_threads = 8"
ON="use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_variable_length_types = 0"
OPT_IN="use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_variable_length_types = 1"
OFF="use_top_k_dynamic_filtering = 0, use_top_k_dynamic_filtering_for_variable_length_types = 0"

# 20 row groups of 1000 rows with disjoint ranges of k, not in the order of k: the smallest k are in row
# group 11, the largest in row group 8. `s` sorts as k; `d` has ties, also between row groups; `n` has a
# NULL in row groups 0, 5, 10 and 15; `e` is '' in every tenth row, so all row groups share the minimum.
DATA="SELECT leftPad(toString(k), 6, '0') AS s, number AS v, leftPad(toString(intDiv(k + 5, 10)), 5, '0') AS d,
        if(number % 5000 = 7, NULL, s) AS n, if(number % 10 = 0, '', s) AS e
    FROM (SELECT (number % 1000) + 1000 * ((intDiv(number, 1000) * 7 + 3) % 20) AS k, number FROM numbers(20000))"
"${LOCAL[@]}" --query "
    INSERT INTO FUNCTION file('${DIR}/t.parquet', Parquet) ${DATA}
    SETTINGS output_format_parquet_row_group_size = 1000, engine_file_truncate_on_insert = 1;

    -- An Int64 column read as String: the string order differs from the numeric order of the statistics.
    INSERT INTO FUNCTION file('${DIR}/i.parquet', Parquet)
    SELECT toInt64(multiIf(number < 10, 100 + number, number < 20, number - 5, number)) AS x, number AS y FROM numbers(30)
    SETTINGS output_format_parquet_row_group_size = 10, engine_file_truncate_on_insert = 1;

    -- FixedString(4) is written as FIXED_LEN_BYTE_ARRAY, whose statistics keep the trailing zero bytes that a
    -- read as String strips. Row group 0 has f = 'b', row groups 1 to 5 f = 'ab'; only the last one has v < 1000.
    INSERT INTO FUNCTION file('${DIR}/f.parquet', Parquet)
    SELECT toFixedString(if(number < 1000, 'b', 'ab'), 4) AS f, if(number < 5000, 1000 + number % 1000, number % 1000) AS v
    FROM numbers(6000)
    SETTINGS output_format_parquet_row_group_size = 1000, max_threads = 1, max_insert_threads = 1, engine_file_truncate_on_insert = 1;

    SELECT num_row_groups FROM file('${DIR}/t.parquet', ParquetMetadata)"

# $@: pairs of a query and a Python condition on its `rows_read`
run_json() {
    local script="" conditions=()
    while [ $# -gt 0 ]; do
        script+="$1; "
        conditions+=("$2")
        shift 2
    done
    "${LOCAL[@]}" --query "SET ${ST}, ${ON}; ${script}" --format JSON | python3 -c "
import sys, json
text, end, decoder = sys.stdin.read(), 0, json.JSONDecoder()
for condition in sys.argv[1:]:
    while text[end].isspace():
        end += 1
    d, end = decoder.raw_decode(text, end)
    rows_read = d['statistics']['rows_read']
    print([list(r.values()) for r in d['data']], condition + ':', eval(condition))
" "${conditions[@]}"
}

# Runs all of ${queries} without the optimization, then with it and with the opt-in with one thread, and with it
# with 8 threads, and checks every result against the one without the optimization.
compare() {
    local script="" mode query
    for mode in "${ST}, ${OFF}" "${ST}, ${ON}" "${ST}, ${OPT_IN}" "${MT}, ${ON}"; do
        # Without the query condition cache, so that no mode reads the entries of an earlier one.
        script+="SET ${mode}, use_query_condition_cache = 0; "
        for query in "${queries[@]}"; do
            script+="${query}; SELECT '---'; "
        done
    done
    "${LOCAL[@]}" --query "${script}" | python3 -c "
import sys
queries = sys.argv[1:]
results, lines = [], []
for line in sys.stdin.read().split('\n'):
    if line == '---':
        results.append(lines)
        lines = []
    else:
        lines.append(line)
mode = lambda i: results[i * len(queries):(i + 1) * len(queries)]
off = mode(0)
for checked in ((mode(1), mode(2)), (mode(3),)):
    for i, query in enumerate(queries):
        same = i < len(off) and off[i] and all(i < len(m) and m[i] == off[i] for m in checked)
        print('OK' if same else 'FAIL: ' + query)
" "${queries[@]}"
}

echo "-- the row group with the smallest (largest) value is read first and the others are skipped"
# The last one: not for a LIMIT at which the sorting filters rows by its own threshold.
run_json \
    "SELECT s FROM file('${DIR}/t.parquet') ORDER BY s LIMIT 3" "rows_read <= 2000" \
    "SELECT s FROM file('${DIR}/t.parquet') ORDER BY s DESC LIMIT 3" "rows_read <= 2000" \
    "SELECT s FROM file('${DIR}/t.parquet') WHERE v % 2 = 0 ORDER BY s DESC LIMIT 3" "rows_read <= 2000" \
    "SELECT n FROM file('${DIR}/t.parquet') ORDER BY n DESC LIMIT 3" "rows_read <= 6000" \
    "SELECT s FROM file('${DIR}/t.parquet', Parquet, 's LowCardinality(String)') ORDER BY s LIMIT 3" "rows_read <= 2000" \
    "SELECT n FROM file('${DIR}/t.parquet', Parquet, 'n LowCardinality(Nullable(String))') ORDER BY n LIMIT 3" "rows_read <= 6000" \
    "SELECT count() FROM (SELECT s FROM file('${DIR}/t.parquet') ORDER BY s LIMIT 1500)" "rows_read == 20000"

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
compare

echo "-- a Memory table gets the per-row filter only with the opt-in"
"${LOCAL[@]}" --query "
    CREATE TABLE t_mem (s String) ENGINE = Memory;
    INSERT INTO t_mem SELECT toString(number) FROM numbers(10);
    SET ${ON};
    SELECT count() FROM (EXPLAIN actions = 1 SELECT s FROM t_mem ORDER BY s LIMIT 3) WHERE explain LIKE '%TopN filter%';
    SET ${OPT_IN};
    SELECT count() FROM (EXPLAIN actions = 1 SELECT s FROM t_mem ORDER BY s LIMIT 3) WHERE explain LIKE '%TopN filter%'"

# The same data in a table under `user_files` with an mtime in the past, so that the query condition cache trusts
# the file version, in object storage and in a data lake.
S3_FILE="${CLICKHOUSE_DATABASE}_05339/t.parquet"
S3_GLOB="${CLICKHOUSE_DATABASE}_05339_glob"
ICEBERG_DIR="${USER_FILES_PATH}/lakehouses/${CLICKHOUSE_DATABASE}_05339"
rm -rf "${ICEBERG_DIR}"
${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS t_05339;
    CREATE TABLE t_05339 (s String, v UInt64) ENGINE = File(Parquet, '${CLICKHOUSE_DATABASE}/05339.parquet')
        SETTINGS output_format_parquet_row_group_size = 1000, input_format_parquet_filter_push_down = 1,
            input_format_parquet_preserve_order = 0;
    INSERT INTO t_05339 SELECT s, v FROM (${DATA})
        SETTINGS output_format_parallel_formatting = 0, max_threads = 1, max_insert_threads = 1, max_block_size = 65409;

    INSERT INTO FUNCTION s3(s3_conn, filename = '${S3_FILE}', format = Parquet) SELECT s, v FROM (${DATA})
        SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 1000, output_format_parallel_formatting = 0,
            max_threads = 1, max_insert_threads = 1, max_block_size = 65409;
    DROP TABLE IF EXISTS t_05339_s3;
    CREATE TABLE t_05339_s3 (s String, v UInt64) ENGINE = S3(s3_conn, filename = '${S3_FILE}', format = Parquet);

    -- Two objects, read in key order. In each one every row group holds the same values, so its row groups are not
    -- ordered and are checked against the threshold only once they were started. All values of a.parquet are larger
    -- than those of b.parquet, so every row group of b.parquet is skipped.
    INSERT INTO FUNCTION s3(s3_conn, filename = '${S3_GLOB}/a.parquet', format = Parquet)
    SELECT concat('c', leftPad(toString(number % 100), 3, '0')) AS s, number AS v FROM numbers(1000)
        SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 100, output_format_parallel_formatting = 0,
            max_threads = 1, max_insert_threads = 1, max_block_size = 65409;
    INSERT INTO FUNCTION s3(s3_conn, filename = '${S3_GLOB}/b.parquet', format = Parquet)
    SELECT concat('b', leftPad(toString(number % 100), 3, '0')) AS s, number AS v FROM numbers(1000)
        SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 100, output_format_parallel_formatting = 0,
            max_threads = 1, max_insert_threads = 1, max_block_size = 65409;
    DROP TABLE IF EXISTS t_05339_s3_glob;
    CREATE TABLE t_05339_s3_glob (s String, v UInt64) ENGINE = S3(s3_conn, filename = '${S3_GLOB}/*.parquet', format = Parquet);

    DROP TABLE IF EXISTS t_05339_iceberg;
    CREATE TABLE t_05339_iceberg (s String, v Int64) ENGINE = IcebergLocal('${ICEBERG_DIR}', 'Parquet')
        SETTINGS output_format_parquet_row_group_size = 1000, output_format_parallel_formatting = 0;
    INSERT INTO t_05339_iceberg SELECT s, toInt64(v) FROM (${DATA})
        SETTINGS allow_experimental_insert_into_iceberg = 1, max_threads = 1, max_insert_threads = 1, max_block_size = 65409;

    SELECT num_row_groups FROM file('${CLICKHOUSE_DATABASE}/05339.parquet', ParquetMetadata);
    SELECT num_row_groups FROM s3(s3_conn, filename = '${S3_FILE}', format = ParquetMetadata);
"
touch -d '2020-01-01 00:00:00' "${USER_FILES_PATH:?}/${CLICKHOUSE_DATABASE}/05339.parquet"

SETTINGS="use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 10000, max_threads = 1,
    max_parsing_threads = 1, max_block_size = 65409, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1,
    input_format_parquet_filter_push_down = 1, input_format_parquet_preserve_order = 0"
qcc_settings="${SETTINGS}, use_top_k_dynamic_filtering_for_variable_length_types = 0"

reads=""
for arm in "asc 0" "desc 0" "asc 1"; do
    read -r direction opt_in <<< "${arm}"
    reads+="SELECT s FROM t_05339 ORDER BY s ${direction} LIMIT 3 SETTINGS ${SETTINGS}, use_query_condition_cache = 0,
        use_top_k_dynamic_filtering_for_variable_length_types = ${opt_in}, log_comment = 'skip_${direction}_${opt_in}' FORMAT Null; "
done
for type in "Nullable(String)" "LowCardinality(String)" "LowCardinality(Nullable(String))"; do
    reads+="SELECT s FROM file('${CLICKHOUSE_DATABASE}/05339.parquet', Parquet, 's ${type}') ORDER BY s DESC LIMIT 3
        SETTINGS ${SETTINGS}, use_query_condition_cache = 0, use_top_k_dynamic_filtering_for_variable_length_types = 0,
            log_comment = 'skip_desc_0_${type}' FORMAT Null; "
done
# Unlike a TopK read of a `File` table with the per-row filter, it does not use the cache without a filter condition.
reads+="SELECT s FROM t_05339 ORDER BY s DESC LIMIT 3
    SETTINGS ${qcc_settings}, use_query_condition_cache = 1, log_comment = 'no_filter' FORMAT Null; "
# Nor does a read of a data lake table, which uses the cache without a filter condition with the per-row filter.
for opt_in in 0 1; do
    reads+="SELECT s FROM t_05339_iceberg ORDER BY s DESC LIMIT 3
        SETTINGS ${SETTINGS}, use_query_condition_cache = 1, use_query_condition_cache_for_top_k = 1, enable_parallel_replicas = 0,
            use_top_k_dynamic_filtering_for_variable_length_types = ${opt_in}, log_comment = 'iceberg_${opt_in}' FORMAT Null; "
done
reads+="SELECT s FROM t_05339_s3_glob ORDER BY s DESC LIMIT 3 SETTINGS ${SETTINGS}, use_query_condition_cache = 0,
    use_top_k_dynamic_filtering_for_variable_length_types = 0, log_comment = 'glob' FORMAT Null; "
${CLICKHOUSE_CLIENT} --query "${reads}"

# The predicate is false only in row group 8, which holds the largest s, so the read for DESC skips row groups
# whose rows match it. Another test can drop the cache at any time, hence the attempts, each with a new predicate.
declare -A cache_counts
# $1: table, $2: 1 to also count after a read without use_query_condition_cache_for_top_k
read_with_query_condition_cache() {
    local queries="" attempt predicate counts without_cache count correct=1
    for attempt in 1 2; do
        predicate="intDiv(v, 1000) != 8 AND v != ${attempt}000000"
        queries+="
            SELECT s FROM $1 WHERE ${predicate} ORDER BY s DESC LIMIT 3
                SETTINGS ${qcc_settings}, use_query_condition_cache = 1 FORMAT Null;
            SELECT count() FROM $1 WHERE ${predicate}
                SETTINGS ${qcc_settings}, use_query_condition_cache = 1, log_comment = '$1_count';
            SELECT s FROM $1 WHERE ${predicate} ORDER BY s DESC LIMIT 3
                SETTINGS ${qcc_settings}, use_query_condition_cache = 1, use_query_condition_cache_for_top_k = 0,
                    log_comment = '$1_topk' FORMAT Null;"
        if [ "$2" = 1 ]; then
            predicate="intDiv(v, 1000) != 8 AND v != ${attempt}100000"
            queries+="
            SELECT s FROM $1 WHERE ${predicate} ORDER BY s DESC LIMIT 3
                SETTINGS ${qcc_settings}, use_query_condition_cache = 1, use_query_condition_cache_for_top_k = 0 FORMAT Null;
            SELECT count() FROM $1 WHERE ${predicate}
                SETTINGS ${qcc_settings}, use_query_condition_cache = 1, log_comment = '$1_written';"
        fi
    done
    counts=$(${CLICKHOUSE_CLIENT} --query "${queries}
        SELECT count() FROM $1 WHERE intDiv(v, 1000) != 8 AND v != 1000000 SETTINGS ${qcc_settings}, use_query_condition_cache = 0")
    without_cache=$(tail -n 1 <<< "${counts}")
    for count in $(head -n -1 <<< "${counts}"); do
        [ "${count}" = "${without_cache}" ] || correct=0
    done
    cache_counts[$1]="$1: count without the cache: ${without_cache}
$1: count with the cache is the count without it: ${correct}"
}
read_with_query_condition_cache t_05339 1
read_with_query_condition_cache t_05339_s3 1
read_with_query_condition_cache t_05339_s3_glob 0

# One scan of `system.query_log`, which is slow.
${CLICKHOUSE_CLIENT} --query "
    SYSTEM FLUSH LOGS query_log;
    CREATE TABLE log_05339 ENGINE = Memory AS SELECT log_comment, ProfileEvents FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish';"

# $1: table
report_query_condition_cache() {
    echo "${cache_counts[$1]}"
    ${CLICKHOUSE_CLIENT} --query "
        SELECT '$1: the count is read from the cache:', max(ProfileEvents['QueryConditionCacheHits'] > 0)
        FROM log_05339 WHERE log_comment = '$1_count';
        SELECT '$1: the read consults the cache without use_query_condition_cache_for_top_k:',
            max(ProfileEvents['QueryConditionCacheHits'] > 0)
        FROM log_05339 WHERE log_comment = '$1_topk';"
}

echo "-- the skipped row groups are counted, and no rows are filtered by the threshold"
${CLICKHOUSE_CLIENT} --query "
    SELECT replaceOne(log_comment, 'skip_', ''),
        ProfileEvents['ParquetTopKSkippedRowGroups'] BETWEEN 17 AND 19,
        ProfileEvents['ParquetRowsFilterExpression'] = 0
    FROM log_05339 WHERE startsWith(log_comment, 'skip_')
    ORDER BY log_comment;"

echo "-- for the query condition cache such a read is a plain read"
report_query_condition_cache t_05339
report_query_condition_cache t_05339_s3
${CLICKHOUSE_CLIENT} --query "
    SELECT 't_05339: the read populates the cache without use_query_condition_cache_for_top_k:',
        max(ProfileEvents['QueryConditionCacheHits'] > 0)
    FROM log_05339 WHERE log_comment = 't_05339_written';
    SELECT 't_05339_s3: the read populates the cache without use_query_condition_cache_for_top_k:',
        max(ProfileEvents['QueryConditionCacheHits'] > 0)
    FROM log_05339 WHERE log_comment = 't_05339_s3_written';
    SELECT 't_05339: a read without a filter does not use the cache:',
        ProfileEvents['QueryConditionCacheHits'] + ProfileEvents['QueryConditionCacheMisses'] = 0
    FROM log_05339 WHERE log_comment = 'no_filter';
    SELECT 't_05339_iceberg: the read uses the cache, with the opt-in ' || splitByChar('_', log_comment)[-1] || ':',
        ProfileEvents['QueryConditionCacheHits'] + ProfileEvents['QueryConditionCacheMisses'] > 0
    FROM log_05339 WHERE startsWith(log_comment, 'iceberg_')
    ORDER BY log_comment;"

echo "-- in object storage, also for row groups skipped after the read of their file has started"
${CLICKHOUSE_CLIENT} --query "
    SELECT ProfileEvents['ParquetTopKSkippedRowGroups'], ProfileEvents['ParquetRowsFilterExpression']
    FROM log_05339 WHERE log_comment = 'glob';"
report_query_condition_cache t_05339_s3_glob

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05339; DROP TABLE t_05339_s3; DROP TABLE t_05339_s3_glob; DROP TABLE t_05339_iceberg; DROP TABLE log_05339"
rm -f "${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}/05339.parquet"
rm -rf "${ICEBERG_DIR}"
