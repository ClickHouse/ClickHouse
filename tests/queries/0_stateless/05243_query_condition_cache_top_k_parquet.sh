#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: needs Parquet
# Tag no-parallel: asserts `QueryConditionCacheHits` on the instance-wide query condition cache,
# which a parallel sibling test can wipe at any moment (see 04498_query_condition_cache_local_files).

# The query condition cache for `ORDER BY ... LIMIT n` (TopK) reads of Parquet files through the
# `File` engine. The first run records the row groups that hold no row of the result, a rerun of
# the same query skips them. The entries depend on the running TopK threshold, which comes from
# the rows of every file the query reads, so they are keyed by the TopK plan, the predicate, and
# the version tokens of all the files. Rewriting one file of a glob must invalidate the entries of
# the other, unchanged files too - otherwise the rows of the unchanged file that now belong to the
# result would stay skipped.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR="${CLICKHOUSE_DATABASE}_05243"
mkdir -p "${USER_FILES_PATH:?}/${DATA_DIR}"
rm -f "${USER_FILES_PATH}/${DATA_DIR}"/*.parquet

# `a.parquet` holds the largest values, `b.parquet` smaller ones; each file keeps its largest values in
# its first row group. So all row groups but the first one of `a.parquet` hold no row of the result of
# `ORDER BY k DESC LIMIT 3`.
function write_file()
{
    ${CLICKHOUSE_CLIENT} --query "
        INSERT INTO FUNCTION file('${DATA_DIR}/$1.parquet')
        SELECT if(number < 1000, $2 + number, number % 1000) AS k, toString(k) AS s FROM numbers(100000)
        SETTINGS output_format_parquet_row_group_size = 1000, engine_file_truncate_on_insert = 1"
}
write_file a 1000000
write_file b 100000

# Backdate the files so their version tokens have settled and the cache engages.
touch -d '2020-01-01 00:00:00' "${USER_FILES_PATH}/${DATA_DIR}"/*.parquet

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_05243"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_05243 (k UInt64, s String) ENGINE = File(Parquet, '${DATA_DIR}/*.parquet')"

SETTINGS="use_query_condition_cache = 1, use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 1000"

function run()
{
    local name=$1
    local query=$2
    local extra_settings=${3:-}
    ${CLICKHOUSE_CLIENT} --query_id "${CLICKHOUSE_TEST_UNIQUE_NAME}_${name}" --query "$query SETTINGS ${SETTINGS}${extra_settings}"
}

# Per query: whether it consulted the cache at all, and whether it found entries there.
function events()
{
    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
    for name in "$@"
    do
        ${CLICKHOUSE_CLIENT} --query "
            SELECT '${name}',
                ProfileEvents['QueryConditionCacheHits'] + ProfileEvents['QueryConditionCacheMisses'] > 0,
                ProfileEvents['QueryConditionCacheHits'] > 0
            FROM system.query_log
            WHERE query_id = '${CLICKHOUSE_TEST_UNIQUE_NAME}_${name}' AND current_database = currentDatabase() AND type = 'QueryFinish'"
    done
}

echo "--- no predicate"
run nowhere_1 "SELECT k FROM t_05243 ORDER BY k DESC LIMIT 3"
run nowhere_2 "SELECT k FROM t_05243 ORDER BY k DESC LIMIT 3"
# A different LIMIT is a different TopK plan: it must not reuse the entries.
run nowhere_limit "SELECT k FROM t_05243 ORDER BY k DESC LIMIT 4"
# The TopK entries must not reach a plain read, nor a TopK read with the other direction.
run nowhere_plain "SELECT count(), sum(k) FROM t_05243"
run nowhere_asc "SELECT k FROM t_05243 ORDER BY k LIMIT 3"

echo "--- with a predicate"
run where_1 "SELECT k, s FROM t_05243 WHERE k % 3 = 0 ORDER BY k DESC LIMIT 3"
run where_2 "SELECT k, s FROM t_05243 WHERE k % 3 = 0 ORDER BY k DESC LIMIT 3"
run where_plain "SELECT count() FROM t_05243 WHERE k % 3 = 0"

echo "--- one file of the glob is rewritten"
# Now `a.parquet` holds the smallest values, and the result comes from the first row group of the
# unchanged `b.parquet`, which the entries above recorded as holding no row of the result.
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${DATA_DIR}/a.parquet') SELECT number % 10 AS k, toString(k) AS s FROM numbers(100000)
    SETTINGS output_format_parquet_row_group_size = 1000, engine_file_truncate_on_insert = 1"
touch -d '2020-01-02 00:00:00' "${USER_FILES_PATH}/${DATA_DIR}/a.parquet"
run rewritten_nowhere "SELECT k FROM t_05243 ORDER BY k DESC LIMIT 3"
run rewritten_where "SELECT k, s FROM t_05243 WHERE k % 3 = 0 ORDER BY k DESC LIMIT 3"

echo "--- a filter on _file"
# Only the files the query reads make the threshold, so a change of another file must not invalidate
# the entries of a query that filters it out.
run file_b_1 "SELECT k FROM t_05243 WHERE _file = 'b.parquet' ORDER BY k DESC LIMIT 3"
run file_b_2 "SELECT k FROM t_05243 WHERE _file = 'b.parquet' ORDER BY k DESC LIMIT 3"
touch -d '2020-01-03 00:00:00' "${USER_FILES_PATH}/${DATA_DIR}/a.parquet"
run file_b_after_a_changed "SELECT k FROM t_05243 WHERE _file = 'b.parquet' ORDER BY k DESC LIMIT 3"

echo "--- external sorting"
# With external sorting the sorted result comes out of the merge of the spilled blocks.
EXTERNAL_SORT="max_bytes_before_external_sort = 1, max_bytes_ratio_before_external_sort = 0"
run external_1 "SELECT k FROM t_05243 ORDER BY k DESC LIMIT 4" ", ${EXTERNAL_SORT}"
run external_2 "SELECT k FROM t_05243 ORDER BY k DESC LIMIT 4" ", ${EXTERNAL_SORT}"

echo "--- a file changes after the cache was used"
# The entries under the key may only be used while every file the query reads is in the version the
# key was made for. The failpoint stands in for a file rewritten after an entry was used for another
# file: the query must fail rather than return what those entries left of the result.
# First record the entries for the current versions of the files.
${CLICKHOUSE_CLIENT} --query "SELECT k FROM t_05243 ORDER BY k DESC LIMIT 3 SETTINGS ${SETTINGS} FORMAT Null"
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT file_top_k_query_condition_cache_inject_file_change"
${CLICKHOUSE_CLIENT} --query "SELECT k FROM t_05243 ORDER BY k DESC LIMIT 3 SETTINGS ${SETTINGS}" 2>&1 | grep -o -m1 FILE_CHANGED_DURING_READ
${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT file_top_k_query_condition_cache_inject_file_change"
${CLICKHOUSE_CLIENT} --query "SELECT k FROM t_05243 ORDER BY k DESC LIMIT 3 SETTINGS ${SETTINGS}"

echo "--- use_query_condition_cache_for_top_k = 0"
run gate_1 "SELECT k FROM t_05243 ORDER BY k DESC LIMIT 5" ", use_query_condition_cache_for_top_k = 0"
run gate_2 "SELECT k FROM t_05243 ORDER BY k DESC LIMIT 5" ", use_query_condition_cache_for_top_k = 0"

echo "--- query condition cache lookups"
events nowhere_1 nowhere_2 nowhere_limit nowhere_asc where_1 where_2 rewritten_nowhere rewritten_where file_b_1 file_b_2 file_b_after_a_changed external_1 external_2 gate_1 gate_2

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05243"
rm -rf "${USER_FILES_PATH:?}/${DATA_DIR}"
