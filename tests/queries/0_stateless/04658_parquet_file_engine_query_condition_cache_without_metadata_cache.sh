#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: needs Parquet
# Tag no-parallel: the query condition cache is server-wide and size-bounded, so a
# concurrent test can evict our entry between the two reads and turn the expected
# hit into a miss (same reason as `04637_parquet_file_engine_bucketed_query_condition_cache`)

# The query condition cache and the Parquet metadata cache are independent caches with
# independent settings. Regression test for the local-file read path (`StorageFile`):
# the query-condition-cache lookup used to be gated on `object_with_metadata`, which is
# only constructed under `use_parquet_metadata_cache`, so `use_parquet_metadata_cache = 0`
# silently disabled query-condition-cache reads while the write path kept populating the
# cache - the second identical query re-read every row group instead of hitting.
# Like `04637_parquet_file_engine_bucketed_query_condition_cache`, this needs a
# `File`-engine table in an `Atomic` database: table functions carry a nil storage UUID
# for which `QueryConditionCache::{read,write}` are no-ops.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SETTINGS="use_query_condition_cache = 1, use_parquet_metadata_cache = 0, parallelize_output_from_storages = 1, max_threads = 1"

TAG="04658_${CLICKHOUSE_DATABASE}"
DATA_FILE_RELATIVE="${CLICKHOUSE_TEST_UNIQUE_NAME}/04658.parquet"

${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION file('${DATA_FILE_RELATIVE}') SELECT number FROM numbers(3200) SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_row_group_size = 50"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_04658"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_04658 (number UInt64) ENGINE = File(Parquet, '${DATA_FILE_RELATIVE}')"

# The cache is bypassed until the file's version token has settled
# (`file_version_settle_seconds = 3` in `StorageFile.cpp`), so give the file time to settle.
sleep 4

# 1. First filtered read with the metadata cache disabled: the query-condition-cache
# lookup must MISS (proving the lookup is consulted at all) and then populate the cache.
${CLICKHOUSE_CLIENT} --query "SELECT number FROM t_04658 WHERE number = 3175 SETTINGS ${SETTINGS}, log_comment = '${TAG}_first'"

# 2. Identical repeat: the lookup must HIT the entry written by (1), even though the
# Parquet metadata cache stays disabled.
${CLICKHOUSE_CLIENT} --query "SELECT number FROM t_04658 WHERE number = 3175 SETTINGS ${SETTINGS}, log_comment = '${TAG}_second'"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

${CLICKHOUSE_CLIENT} --query "
    SELECT
        replaceOne(log_comment, '${TAG}_', ''),
        ProfileEvents['QueryConditionCacheHits'] > 0 AS hit,
        ProfileEvents['QueryConditionCacheMisses'] > 0 AS miss
    FROM system.query_log
    WHERE current_database = currentDatabase()
        AND type = 'QueryFinish'
        AND query_kind = 'Select'
        AND log_comment LIKE '${TAG}_%'
    ORDER BY event_time_microseconds"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_04658"
rm -r "${CLICKHOUSE_USER_FILES_UNIQUE:?}"
