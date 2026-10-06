#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: needs Parquet and S3
# Tag no-parallel: asserts `QueryConditionCacheHits` on the instance-wide query condition cache, which a parallel
# test can clear at any moment (see 04498_query_condition_cache_local_files.sh).

# Every row group below has a row matching `k % 100000 < 100` (each one spans at least 100000 rows, and the last row
# matches), so no row group can be skipped; a repeated query must still skip the 8192-row blocks without a match.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${USER_FILES_PATH:?}/${CLICKHOUSE_DATABASE}/05331_query_condition_cache.parquet"
SETTINGS="use_query_condition_cache = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1"

function run()
{
    local table=$1 condition=$2 tag=$3
    for i in 1 2; do
        ${CLICKHOUSE_CLIENT} --query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_${tag}_$i" --query "
            SELECT count(), sum(v) FROM $table WHERE $condition SETTINGS $SETTINGS"
    done
    ${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(v) FROM $table WHERE $condition SETTINGS use_query_condition_cache = 0"
}

# The second run is a cache hit and evaluates PREWHERE on far fewer rows than the first one.
function check_second_run()
{
    local tag=$1
    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
    ${CLICKHOUSE_CLIENT} --query "
        SELECT
            maxIf(ProfileEvents['QueryConditionCacheHits'], endsWith(query_id, '_2')) > 0,
            maxIf(ProfileEvents['ParquetRowsFilterExpression'], endsWith(query_id, '_2')) * 4
                < maxIf(ProfileEvents['ParquetRowsFilterExpression'], endsWith(query_id, '_1'))
        FROM system.query_log
        WHERE current_database = currentDatabase() AND type = 'QueryFinish'
            AND startsWith(query_id, '${CLICKHOUSE_TEST_UNIQUE_NAME}_${tag}_')"
}

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t (k UInt64, v UInt64)
    ENGINE = File(Parquet, '${CLICKHOUSE_DATABASE}/05331_query_condition_cache.parquet')
    SETTINGS output_format_parquet_row_group_size = 100000"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t SELECT number, number * 2 FROM numbers(1000001)"
# The cache is used only for a file whose modification time has settled.
touch -d '2020-01-01 00:00:00' "$DATA_FILE"

echo "File"
run t "k % 100000 < 100" file
check_second_run file
run t "k IN (5, 99999, 500000)" in

echo "File, rewritten so that the skipped blocks match"
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO t SELECT number + 50000, number * 2 FROM numbers(1000001) SETTINGS engine_file_truncate_on_insert = 1"
touch -d '2020-01-02 00:00:00' "$DATA_FILE"
run t "k % 100000 < 100" rewrite

echo "S3"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ts (k UInt64, v UInt64)
    ENGINE = S3(s3_conn, filename = '${CLICKHOUSE_DATABASE}_05331_query_condition_cache.parquet', format = Parquet)
    SETTINGS output_format_parquet_row_group_size = 100000"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ts SELECT number, number * 2 FROM numbers(1000001) SETTINGS s3_truncate_on_insert = 1"
run ts "k % 100000 < 100" s3
check_second_run s3

${CLICKHOUSE_CLIENT} --query "DROP TABLE t"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ts"
rm -f "$DATA_FILE"
