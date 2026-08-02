#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs Parquet

# `use_parquet_metadata_cache` must only control `ParquetMetadataCache`; it must not
# affect the Query Condition Cache on the local-file read path (`StorageFile`). The two
# caches are independent, as they already are in `StorageObjectStorageSource`.
#
# Regression coverage: the cache lookup in `StorageFile::generate` used to be gated on the
# presence of `object_with_metadata`, which is itself built only under
# `use_parquet_metadata_cache`. With `use_parquet_metadata_cache = 0` the population still
# ran, so a filtered read wrote an entry that no later read ever consulted - every repeated
# filtered query re-read all row groups, silently losing the pruning.
#
# A `File`-engine table in an `Atomic` database is used rather than the `file` table
# function because the latter carries a nil storage UUID, for which
# `QueryConditionCache::read` and `QueryConditionCache::write` are no-ops (see
# `04637_parquet_file_engine_bucketed_query_condition_cache`).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# 3200 rows at row-group size 50 => 64 row groups, so a single-value predicate leaves most
# of them prunable and the cache has something to record.
#
# The file is staged through the `file` table function and the `File`-engine table is created
# over that path: an `INSERT` into the table itself goes through the engine's own write
# pipeline, which does not apply the query-level `output_format_parquet_row_group_size` and
# would produce a single row group.
TAG="04656_${CLICKHOUSE_DATABASE}"
DATA_FILE_RELATIVE="${CLICKHOUSE_TEST_UNIQUE_NAME}/04656.parquet"

# `use_parquet_metadata_cache = 0` is the point of the test; reads stay non-bucketed
# (`max_threads = 1`) because a bucketed source bypasses the query condition cache by design.
SETTINGS="use_query_condition_cache = 1, use_parquet_metadata_cache = 0, parallelize_output_from_storages = 1, max_threads = 1"

${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION file('${DATA_FILE_RELATIVE}') SELECT number FROM numbers(3200) SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_row_group_size = 50"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_04656"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_04656 (number UInt64) ENGINE = File(Parquet, '${DATA_FILE_RELATIVE}')"

# The cache is bypassed until the file's version token has settled (the last modification must
# be comfortably in the past for the token to prove a later rewrite,
# `file_version_settle_seconds = 3` in `StorageFile.cpp`), so give the file time to settle.
sleep 4

# 1. First filtered read with the metadata cache off: the lookup must MISS (not be skipped
# altogether) and populate the entry.
${CLICKHOUSE_CLIENT} --query "SELECT number FROM t_04656 WHERE number = 3175 SETTINGS ${SETTINGS}, log_comment = '${TAG}_first'"

# 2. Repeat: the lookup must HIT the entry written by (1). Before the fix this recorded
# neither a hit nor a miss, because the lookup was disabled together with the metadata cache.
${CLICKHOUSE_CLIENT} --query "SELECT number FROM t_04656 WHERE number = 3175 SETTINGS ${SETTINGS}, log_comment = '${TAG}_second'"

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

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_04656"
rm -r "${CLICKHOUSE_USER_FILES_UNIQUE:?}"
