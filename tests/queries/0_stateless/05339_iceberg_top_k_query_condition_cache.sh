#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-parallel-replicas
# Tag no-fasttest: needs Parquet and Iceberg (USE_AVRO)
# Tag no-parallel: asserts `QueryConditionCacheHits` on the instance-wide query condition cache,
# which a parallel sibling test can wipe at any moment (see 05243_query_condition_cache_top_k_parquet).
# Tag no-parallel-replicas: asserts on `ProfileEvents` of the initial query, but with parallel replicas the replicas use the query condition cache

# The query condition cache for `ORDER BY ... LIMIT n` (TopK) reads of Iceberg tables. The first run
# records the row groups that hold no row of the result, a rerun of the same query skips them. The
# entries depend on the running TopK threshold, which comes from the rows of every file the query
# reads, so they are keyed by the TopK plan, the predicate and the snapshot of the table. A new
# snapshot must not reuse them: after a delete, the result comes from row groups that the entries of
# the previous snapshot recorded as holding no row of the result.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ICEBERG_DIR="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_05339"
rm -rf "${ICEBERG_DIR}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_05339"
# The Iceberg sink writes with the format settings of the table: those of the server plus its `SETTINGS` clause.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_05339 (k Int64, s String) ENGINE = IcebergLocal('${ICEBERG_DIR}', 'Parquet')
    SETTINGS output_format_parquet_row_group_size = 1000"

# The first data file holds the largest values, the second one smaller ones; each file keeps its largest
# values in its first row group. So all row groups but the first one of the first file hold no row of the
# result of `ORDER BY k DESC LIMIT 3`.
function insert()
{
    ${CLICKHOUSE_CLIENT} --allow_experimental_insert_into_iceberg 1 --max_insert_threads 1 --query "
        INSERT INTO t_05339 SELECT if(number < 1000, $1 + number, number % 1000) AS k, toString(k) AS s FROM numbers(100000)"
}
insert 1000000
insert 100000

SETTINGS="use_query_condition_cache = 1, use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 1000,
    input_format_parquet_use_native_reader_v3 = 1"

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
run nowhere_1 "SELECT k FROM t_05339 ORDER BY k DESC LIMIT 3"
run nowhere_2 "SELECT k FROM t_05339 ORDER BY k DESC LIMIT 3"
# A different LIMIT is a different TopK plan: it must not reuse the entries.
run nowhere_limit "SELECT k FROM t_05339 ORDER BY k DESC LIMIT 4"
# The TopK entries must not reach a plain read, nor a TopK read with the other direction.
run nowhere_plain "SELECT count(), sum(k) FROM t_05339"
run nowhere_asc "SELECT k FROM t_05339 ORDER BY k LIMIT 3"

echo "--- with a predicate"
run where_1 "SELECT k, s FROM t_05339 WHERE k % 3 = 0 ORDER BY k DESC LIMIT 3"
run where_2 "SELECT k, s FROM t_05339 WHERE k % 3 = 0 ORDER BY k DESC LIMIT 3"
run where_plain "SELECT count() FROM t_05339 WHERE k % 3 = 0"

echo "--- with a predicate on _path"
# The conjunct on `_path` is not pushed into the format, but narrows down the set of files read: without
# the first data file, the result comes from the first row group of the second one, which the entries of
# the queries above recorded as holding no row of the result. So the query must not reuse them.
FIRST_FILE=$(${CLICKHOUSE_CLIENT} --query "SELECT _path FROM t_05339 WHERE k = 1000999")
run where_path_1 "SELECT k, s FROM t_05339 WHERE k % 3 = 0 AND _path != '${FIRST_FILE}' ORDER BY k DESC LIMIT 3"
run where_path_2 "SELECT k, s FROM t_05339 WHERE k % 3 = 0 AND _path != '${FIRST_FILE}' ORDER BY k DESC LIMIT 3"

echo "--- a new snapshot"
# The delete removes the result of the queries above. The new result lies in the first row group of the
# second data file, which the entries of the previous snapshot recorded as holding no row of the result.
${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_05339 DELETE WHERE k >= 1000000 SETTINGS allow_experimental_insert_into_iceberg = 1"
run deleted_nowhere_1 "SELECT k FROM t_05339 ORDER BY k DESC LIMIT 3"
run deleted_where_1 "SELECT k, s FROM t_05339 WHERE k % 3 = 0 ORDER BY k DESC LIMIT 3"
run deleted_nowhere_2 "SELECT k FROM t_05339 ORDER BY k DESC LIMIT 3"

echo "--- use_query_condition_cache_for_top_k = 0"
run gate_1 "SELECT k FROM t_05339 ORDER BY k DESC LIMIT 5" ", use_query_condition_cache_for_top_k = 0"
run gate_2 "SELECT k FROM t_05339 ORDER BY k DESC LIMIT 5" ", use_query_condition_cache_for_top_k = 0"

echo "--- query condition cache lookups"
events nowhere_1 nowhere_2 nowhere_limit nowhere_plain nowhere_asc where_1 where_2 where_path_1 where_path_2 deleted_nowhere_1 deleted_where_1 deleted_nowhere_2 gate_1 gate_2

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05339"
rm -rf "${ICEBERG_DIR}"
