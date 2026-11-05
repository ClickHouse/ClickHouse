#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs Parquet

# The query condition cache key of an `ORDER BY ... LIMIT n` (TopK) read of a Parquet file without a
# predicate covers the settings deciding which physical column the sort column is bound to. A read
# matching the column `K` of the file case-insensitively prunes row groups by its values; a read
# matching case-sensitively finds no column `k` and fills it with defaults, so it must not reuse
# the entries of the first one.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR="${CLICKHOUSE_DATABASE}_05353"
mkdir -p "${USER_FILES_PATH:?}/${DATA_DIR}"
rm -f "${USER_FILES_PATH}/${DATA_DIR}"/*.parquet

${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${DATA_DIR}/a.parquet')
    SELECT if(number < 1000, 1000000 + number, number % 1000) AS K FROM numbers(100000)
    SETTINGS output_format_parquet_row_group_size = 1000, engine_file_truncate_on_insert = 1"

# Backdate the file so its version token has settled and the cache engages.
touch -d '2020-01-01 00:00:00' "${USER_FILES_PATH}/${DATA_DIR}"/a.parquet

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_05353"
# A table over a table function has a UUID, and its reads take the format settings of the query
# (a `File` table fixes them at `CREATE`).
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_05353 AS file('${DATA_DIR}/a.parquet', Parquet, 'k UInt64')"

SETTINGS="use_query_condition_cache = 1, use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 1000, input_format_parquet_allow_missing_columns = 1"

function run()
{
    local name=$1
    local case_insensitive=$2
    ${CLICKHOUSE_CLIENT} --query_id "${CLICKHOUSE_TEST_UNIQUE_NAME}_${name}" --query "
        SELECT k FROM t_05353 ORDER BY k DESC LIMIT 3
        SETTINGS ${SETTINGS}, input_format_parquet_case_insensitive_column_matching = ${case_insensitive}"
}

run insensitive 1
run sensitive 0

# The case-sensitive read consulted the cache and found no entries of the case-insensitive one.
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT ProfileEvents['QueryConditionCacheHits'] + ProfileEvents['QueryConditionCacheMisses'] > 0,
        ProfileEvents['QueryConditionCacheHits'] > 0
    FROM system.query_log
    WHERE query_id = '${CLICKHOUSE_TEST_UNIQUE_NAME}_sensitive' AND current_database = currentDatabase() AND type = 'QueryFinish'"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05353"
rm -rf "${USER_FILES_PATH:?}/${DATA_DIR}"
