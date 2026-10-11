#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: needs Parquet
# Tag no-parallel: the query condition cache is server-wide and size-bounded, so a
# concurrent test can evict our entry between the two plain reads and turn the
# expected hit into a miss

# Coverage for the query-condition-cache guards on the bucketed local-file read path
# (`StorageFile`), on a surface with a real storage UUID. The `file()` / `s3Cluster()`
# table functions carry a nil storage UUID, for which `QueryConditionCache::{read,write}`
# are no-ops, so tests built on them (e.g. `04402_parquet_bucketed_query_condition_cache`)
# cannot observe the cache. A `File`-engine table in an `Atomic` database has a real UUID,
# so here the cache genuinely engages and the guards are observable per query through the
# `QueryConditionCacheHits` / `QueryConditionCacheMisses` profile events (per query, so
# no global cache inspection is needed).
#
# The invariant: a source assigned one bucket of a parallel single-file split
# (`file_bucket_info` is set) must neither consult nor populate the query condition
# cache. `getMatchedBuckets` only reports the row groups that matched inside this
# bucket while the recorded `total_groups` is the whole file, so writing would store
# the row groups owned by other buckets as "unmatched" under the whole-file key, and a
# later read could skip valid row groups and return incomplete results.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The splitter needs >= 32 row groups to fan out (min 16 row groups per chunk, at least
# 2 chunks): 3200 rows at row-group size 50 => 64 row groups. The file is far below the
# default `input_format_parquet_min_bytes_to_split`, so bucketed queries set that and
# `input_format_parquet_bytes_per_split_bucket` to 0 to opt out of the size-based gate.
#
# The multi-row-group Parquet file is staged through the `file` table function and the
# `File`-engine table is created over that path: an `INSERT` into the table itself would
# go through the engine's own write pipeline, which does not apply the query-level
# `output_format_parquet_row_group_size` and produces a single row group - nothing to
# split, prune, or cache.
BUCKET_SETTINGS="use_query_condition_cache = 1, parallelize_output_from_storages = 1, max_threads = 8, input_format_parquet_min_bytes_to_split = 0, input_format_parquet_bytes_per_split_bucket = 0"
PLAIN_SETTINGS="use_query_condition_cache = 1, parallelize_output_from_storages = 1, max_threads = 1"

TAG="04637_${CLICKHOUSE_DATABASE}"
DATA_FILE_RELATIVE="${CLICKHOUSE_TEST_UNIQUE_NAME}/04637.parquet"

${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION file('${DATA_FILE_RELATIVE}') SELECT number FROM numbers(3200) SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_row_group_size = 50"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_04637"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_04637 (number UInt64) ENGINE = File(Parquet, '${DATA_FILE_RELATIVE}')"

# The cache is bypassed until the file's version token has settled (the last
# modification must be comfortably in the past for the token to prove a later rewrite,
# `file_version_settle_seconds = 3` in `StorageFile.cpp`), so give the file time to settle.
sleep 4

# A predicate matching a single value in the last row group (3175 lands in row group 63).

# 1. Bucketed filtered read: must return the full result and must neither consult nor
# populate the cache (no hit and no miss recorded).
${CLICKHOUSE_CLIENT} --query "SELECT number FROM t_04637 WHERE number = 3175 SETTINGS ${BUCKET_SETTINGS}, log_comment = '${TAG}_bucketed_cold'"

# 2. Non-bucketed read of the same predicate: the lookup must MISS - if the bucketed
# read above had (incorrectly) populated the whole-file key, this would be a hit on a
# poisoned entry - and the full result proves no row groups were skipped. This read
# then populates the cache.
${CLICKHOUSE_CLIENT} --query "SELECT number FROM t_04637 WHERE number = 3175 SETTINGS ${PLAIN_SETTINGS}, log_comment = '${TAG}_plain_first'"

# 3. Non-bucketed repeat: the lookup must HIT the entry written by (2) - this proves the
# cache genuinely engages on this surface (real UUID), i.e. the assertions above are not
# vacuous - and the result is still complete.
${CLICKHOUSE_CLIENT} --query "SELECT number FROM t_04637 WHERE number = 3175 SETTINGS ${PLAIN_SETTINGS}, log_comment = '${TAG}_plain_second'"

# 4. Bucketed read with the whole-file entry now present: the read guard must keep
# ignoring the cache (no hit recorded) and the sources read exactly their planned
# assignment.
${CLICKHOUSE_CLIENT} --query "SELECT number FROM t_04637 WHERE number = 3175 SETTINGS ${BUCKET_SETTINGS}, log_comment = '${TAG}_bucketed_warm'"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

# `EngineFileLikeReadFiles` counts one per source that opened the file: > 1 proves the
# bucketed queries really were fanned out into a parallel single-file split (and the
# hit/miss assertions are about the bucketed path, not about an unsplit fallback read).
${CLICKHOUSE_CLIENT} --query "
    SELECT
        replaceOne(log_comment, '${TAG}_', ''),
        ProfileEvents['QueryConditionCacheHits'] > 0 AS hit,
        ProfileEvents['QueryConditionCacheMisses'] > 0 AS miss,
        ProfileEvents['EngineFileLikeReadFiles'] > 1 AS split
    FROM system.query_log
    WHERE current_database = currentDatabase()
        AND type = 'QueryFinish'
        AND query_kind = 'Select'
        AND log_comment LIKE '${TAG}_%'
    ORDER BY event_time_microseconds"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_04637"
rm -r "${CLICKHOUSE_USER_FILES_UNIQUE:?}"
