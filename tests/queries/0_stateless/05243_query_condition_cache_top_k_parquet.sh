#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: needs Parquet
# Tag no-parallel: asserts `QueryConditionCacheHits` on the instance-wide query condition cache,
# which a parallel sibling test can wipe at any moment (see 04498_query_condition_cache_local_files).

# The query condition cache for `ORDER BY ... LIMIT n` (TopK) reads of Parquet files through the
# `File` engine. The first run records the row groups that held no row of the result, a rerun of
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

# Each file keeps its largest values in its first row group, so once that row group has been read,
# the threshold of `ORDER BY k DESC LIMIT 3` rejects the other row groups of the file.
function write_file()
{
    ${CLICKHOUSE_CLIENT} --query "
        INSERT INTO FUNCTION file('${DATA_DIR}/$1.parquet')
        SELECT if(number < 1000, $2 + number, number % 1000) AS k, toString(k) AS s FROM numbers(100000)
        SETTINGS output_format_parquet_row_group_size = 1000, engine_file_truncate_on_insert = 1"
}
write_file a 0
write_file b 0

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_05243"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_05243 (k UInt64, s String) ENGINE = File(Parquet, '${DATA_DIR}/*.parquet')"

# The files of the glob are read in the order of the directory listing. The one read first gets the
# largest values, so its first row group establishes a threshold which rejects all rows of the other
# file, and the first run records that other file as holding no row of the result.
first=$(${CLICKHOUSE_CLIENT} --query "SELECT _file FROM t_05243 LIMIT 1 SETTINGS max_threads = 1")
first=${first%.parquet}
if [ "$first" = a ]; then second=b; else second=a; fi
write_file "$first" 1000000
write_file "$second" 100000

# Backdate the files so their version tokens have settled and the cache engages.
touch -d '2020-01-01 00:00:00' "${USER_FILES_PATH}/${DATA_DIR}"/*.parquet

# A single parsing thread keeps the reader from decoding the whole file before the first threshold
# is published, so the first run reliably records row groups without a row of the result.
SETTINGS="use_query_condition_cache = 1, use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 1000, max_threads = 1, max_parsing_threads = 1"

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
# Now the file read first holds the smallest values, and the result comes from the first row group
# of the other, unchanged file, which the entries above recorded as holding no row of the result.
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${DATA_DIR}/${first}.parquet') SELECT number % 10 AS k, toString(k) AS s FROM numbers(100000)
    SETTINGS output_format_parquet_row_group_size = 1000, engine_file_truncate_on_insert = 1"
touch -d '2020-01-02 00:00:00' "${USER_FILES_PATH}/${DATA_DIR}/${first}.parquet"
run rewritten_nowhere "SELECT k FROM t_05243 ORDER BY k DESC LIMIT 3"
run rewritten_where "SELECT k, s FROM t_05243 WHERE k % 3 = 0 ORDER BY k DESC LIMIT 3"

echo "--- use_query_condition_cache_for_top_k = 0"
run gate_1 "SELECT k FROM t_05243 ORDER BY k DESC LIMIT 5" ", use_query_condition_cache_for_top_k = 0"
run gate_2 "SELECT k FROM t_05243 ORDER BY k DESC LIMIT 5" ", use_query_condition_cache_for_top_k = 0"

echo "--- query condition cache lookups"
events nowhere_1 nowhere_2 nowhere_limit nowhere_asc where_1 where_2 rewritten_nowhere rewritten_where gate_1 gate_2

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05243"
rm -rf "${USER_FILES_PATH:?}/${DATA_DIR}"
