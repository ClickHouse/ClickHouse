#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: needs Parquet
# Tag no-parallel: asserts `QueryConditionCacheHits` on the instance-wide query condition cache,
# which a parallel sibling test can wipe at any moment (see 04498_query_condition_cache_local_files).

# The query condition cache for `ORDER BY ... LIMIT n` (TopK) reads through the `file` table function,
# whose table has no UUID. Its TopK entries are kept under the synthetic `table_id` of file entries, so
# the first run of a TopK without a predicate records the row groups holding no row of the result and
# a rerun of the same query skips them. A rewrite of the file changes its version token, so the next
# run finds no entries and stays correct.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR="${CLICKHOUSE_DATABASE}_05394"
mkdir -p "${USER_FILES_PATH:?}/${DATA_DIR}"
rm -f "${USER_FILES_PATH}/${DATA_DIR}"/*.parquet

function write_file()
{
    ${CLICKHOUSE_CLIENT} --query "
        INSERT INTO FUNCTION file('${DATA_DIR}/a.parquet')
        SELECT if(number < 1000, $1 + number, number % 1000) AS k FROM numbers(100000)
        SETTINGS output_format_parquet_row_group_size = 1000, engine_file_truncate_on_insert = 1"
    # Backdate the file so its version token has settled and the cache engages.
    touch -d "$2" "${USER_FILES_PATH}/${DATA_DIR}"/a.parquet
}

SETTINGS="use_query_condition_cache = 1, use_query_condition_cache_for_top_k = 1, use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 1000"

function run()
{
    ${CLICKHOUSE_CLIENT} --query_id "${CLICKHOUSE_TEST_UNIQUE_NAME}_$1" --query "
        SELECT k FROM file('${DATA_DIR}/a.parquet', Parquet, 'k UInt64') ORDER BY k DESC LIMIT 3
        SETTINGS ${SETTINGS}"
}

write_file 1000000 '2020-01-01 00:00:00'
run first
run second

write_file 2000000 '2020-01-02 00:00:00'
run rewritten

# Per query: whether it consulted the cache, and whether it found entries (a hit).
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
for name in first second rewritten
do
    ${CLICKHOUSE_CLIENT} --query "
        SELECT '${name}',
            ProfileEvents['QueryConditionCacheHits'] + ProfileEvents['QueryConditionCacheMisses'] > 0,
            ProfileEvents['QueryConditionCacheHits'] > 0
        FROM system.query_log
        WHERE query_id = '${CLICKHOUSE_TEST_UNIQUE_NAME}_${name}' AND current_database = currentDatabase() AND type = 'QueryFinish'"
done

rm -rf "${USER_FILES_PATH:?}/${DATA_DIR}"
