#!/usr/bin/env bash
# Tags: no-fasttest, no-msan, no-parallel
# Tag no-fasttest: delta-kernel-rs is not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan
# Tag no-parallel: asserts `QueryConditionCacheHits` on the instance-wide query condition cache,
# which a parallel sibling test can wipe at any moment (see 05243_query_condition_cache_top_k_parquet).

# The query condition cache for `ORDER BY ... LIMIT n` (TopK) reads of `DeltaLake` tables read with
# Delta Kernel - the counterpart of 05339_iceberg_top_k_query_condition_cache. The entries are keyed by
# the TopK plan, the predicate and the snapshot of the table, so a rerun of the same query reuses them,
# and a new snapshot (another insert) does not.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="${CLICKHOUSE_USER_FILES_UNIQUE}_delta"
rm -rf "${TABLE_PATH}"

DELTA_SETTINGS="--allow_experimental_delta_kernel_rs 1 --allow_delta_lake_writes 1 --allow_delta_lake_create_table 1"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_05394"
# shellcheck disable=SC2086
${CLICKHOUSE_CLIENT} ${DELTA_SETTINGS} --query "
    CREATE TABLE t_05394 (k Int64, s String) ENGINE = DeltaLakeLocal('${TABLE_PATH}', Parquet)
    SETTINGS output_format_parquet_row_group_size = 1000"

# Each data file keeps its largest values in its first row group, so all other row groups hold no row of
# the result of `ORDER BY k DESC LIMIT 3`.
function insert()
{
    # shellcheck disable=SC2086
    ${CLICKHOUSE_CLIENT} ${DELTA_SETTINGS} --output_format_parquet_row_group_size 1000 --max_insert_threads 1 --query "
        INSERT INTO t_05394 SELECT if(number < 1000, $1 + number, number % 1000) AS k, toString(k) AS s FROM numbers(100000)"
}
insert 1000000
insert 100000

SETTINGS="use_query_condition_cache = 1, use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 1000,
    input_format_parquet_use_native_reader_v3 = 1, allow_experimental_delta_kernel_rs = 1"

function run()
{
    local name=$1
    local query=$2
    ${CLICKHOUSE_CLIENT} --query_id "${CLICKHOUSE_TEST_UNIQUE_NAME}_${name}" --query "$query SETTINGS ${SETTINGS}"
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
run nowhere_1 "SELECT k FROM t_05394 ORDER BY k DESC LIMIT 3"
run nowhere_2 "SELECT k FROM t_05394 ORDER BY k DESC LIMIT 3"

echo "--- with a predicate"
run where_1 "SELECT k, s FROM t_05394 WHERE k % 3 = 0 ORDER BY k DESC LIMIT 3"
run where_2 "SELECT k, s FROM t_05394 WHERE k % 3 = 0 ORDER BY k DESC LIMIT 3"

echo "--- a new snapshot"
# The new snapshot must get fresh entries: the first rerun finds none, the second one reuses them.
insert 2000000
run inserted_1 "SELECT k FROM t_05394 ORDER BY k DESC LIMIT 3"
run inserted_2 "SELECT k FROM t_05394 ORDER BY k DESC LIMIT 3"

echo "--- query condition cache lookups"
events nowhere_1 nowhere_2 where_1 where_2 inserted_1 inserted_2

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05394"
rm -rf "${TABLE_PATH}"
