#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: needs Parquet, S3 (MinIO) and Iceberg
# Tag no-parallel: asserts `QueryConditionCacheHits` on the instance-wide query condition cache,
# which a parallel sibling test can wipe at any moment (see 04498_query_condition_cache_local_files).

# The query condition cache for table functions: `file`, `s3` and `icebergLocal` read through a
# table without a UUID. Their entries are keyed by the location of the file together with its
# version (the local version token, or the ETag), which identifies the data without a table. The
# first query with a predicate misses and records the row groups without a match, the second one
# hits. A rewrite of the file changes its version, so the next query misses and stays correct.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR="${CLICKHOUSE_DATABASE}_05256"
mkdir -p "${USER_FILES_PATH:?}/${DATA_DIR}"
ICEBERG_DIR="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_05256"
rm -rf "${ICEBERG_DIR}"

SETTINGS="use_query_condition_cache = 1, enable_analyzer = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1"

function run()
{
    ${CLICKHOUSE_CLIENT} --query_id "${CLICKHOUSE_TEST_UNIQUE_NAME}_$1" --query "$2 SETTINGS ${SETTINGS}"
}

# Per query: the result above, then whether it found cache entries (a hit) and whether it looked
# up the cache in vain (a miss).
function events()
{
    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
    for name in "$@"
    do
        ${CLICKHOUSE_CLIENT} --query "
            SELECT '${name}', ProfileEvents['QueryConditionCacheHits'] > 0, ProfileEvents['QueryConditionCacheMisses'] > 0
            FROM system.query_log
            WHERE query_id = '${CLICKHOUSE_TEST_UNIQUE_NAME}_${name}' AND current_database = currentDatabase() AND type = 'QueryFinish'"
    done
}

echo "--- file"
# All values are even, so the odd value 3 never matches.
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${DATA_DIR}/data.parquet') SELECT number * 2 AS b FROM numbers(1000000)
    SETTINGS output_format_parquet_row_group_size = 100000, engine_file_truncate_on_insert = 1"
# Backdate the file so its version token has settled and the cache engages.
touch -d '2020-01-01 00:00:00' "${USER_FILES_PATH}/${DATA_DIR}/data.parquet"
run file_1 "SELECT count() FROM file('${DATA_DIR}/data.parquet') WHERE b = 3"
run file_2 "SELECT count() FROM file('${DATA_DIR}/data.parquet') WHERE b = 3"
# A rewrite that now contains the value: the stale "no match" entries must not be reused.
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${DATA_DIR}/data.parquet') SELECT number AS b FROM numbers(1000000)
    SETTINGS output_format_parquet_row_group_size = 100000, engine_file_truncate_on_insert = 1"
touch -d '2020-01-02 00:00:00' "${USER_FILES_PATH}/${DATA_DIR}/data.parquet"
run file_rewritten "SELECT count() FROM file('${DATA_DIR}/data.parquet') WHERE b = 3"
events file_1 file_2 file_rewritten

echo "--- s3"
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION s3(s3_conn, filename = '${DATA_DIR}/data.parquet', format = Parquet) SELECT number * 2 AS b FROM numbers(1000000)
    SETTINGS output_format_parquet_row_group_size = 100000, s3_truncate_on_insert = 1"
S3="s3(s3_conn, filename = '${DATA_DIR}/data.parquet', format = Parquet)"
run s3_1 "SELECT count() FROM ${S3} WHERE b = 3"
run s3_2 "SELECT count() FROM ${S3} WHERE b = 3"
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION s3(s3_conn, filename = '${DATA_DIR}/data.parquet', format = Parquet) SELECT number AS b FROM numbers(1000000)
    SETTINGS output_format_parquet_row_group_size = 100000, s3_truncate_on_insert = 1"
run s3_rewritten "SELECT count() FROM ${S3} WHERE b = 3"
events s3_1 s3_2 s3_rewritten

echo "--- s3Cluster split into buckets"
# A reader of one bucket of a file reads only some of its row groups. It must not record the others as
# having no match: the value 850000 is in the ninth row group only, and a later read of the whole
# file with the same predicate would skip it and return 0.
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM s3Cluster('test_shard_localhost', 'http://localhost:11111/test/${DATA_DIR}/data.parquet', 'test', 'testtest', 'Parquet')
    WHERE b = 850000 SETTINGS ${SETTINGS}, cluster_table_function_split_granularity = 'bucket'"
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM s3('http://localhost:11111/test/${DATA_DIR}/data.parquet', 'test', 'testtest', 'Parquet')
    WHERE b = 850000 SETTINGS ${SETTINGS}"

echo "--- icebergLocal"
${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_insert_into_iceberg = 1;
    SET max_insert_threads = 1;
    CREATE TABLE t_05256 (b Int64) ENGINE = IcebergLocal('${ICEBERG_DIR}', 'Parquet');
    INSERT INTO t_05256 SELECT number * 2 FROM numbers(1000000) SETTINGS output_format_parquet_row_group_size = 100000;
"
run iceberg_1 "SELECT count() FROM icebergLocal('${ICEBERG_DIR}') WHERE b = 3"
run iceberg_2 "SELECT count() FROM icebergLocal('${ICEBERG_DIR}') WHERE b = 3"
events iceberg_1 iceberg_2

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05256"
rm -rf "${USER_FILES_PATH:?}/${DATA_DIR}" "${ICEBERG_DIR}"
