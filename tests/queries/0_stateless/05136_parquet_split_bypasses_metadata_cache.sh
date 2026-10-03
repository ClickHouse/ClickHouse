#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: needs Parquet
# Tag no-parallel: this test drops the server-wide Parquet metadata cache and then asserts
# exact hit/miss counts on it, so it would both disturb a concurrent test and be disturbed
# by one (the cache is size-bounded, a concurrent query could evict our entry)

# Coverage for the `!file_bucket_info && !buckets_to_read` gate on the format metadata cache
# in `StorageFileSource::generate`. A source whose read is restricted to a subset of the
# file's row groups - one bucket of a parallel single-file split, or a restriction derived
# from the query condition cache - must parse the footer of the bytes it actually opened
# instead of taking one from `ParquetMetadataCache`. Only then does the footer-digest guard
# (`ParquetFileBucketInfo::footer_digest`) compare the restriction against the real file and
# fail close on an in-place rewrite that the file's version token could not prove; a cached
# footer is the very footer the restriction was computed from, so the guard would end up
# validating the restriction against itself and silently apply a previous generation's
# row-group layout.
#
# The split *decision* in `ReadFromFile::initializePipeline` deliberately does use the cache
# (once per query, which also warms the entry for later queries), so the invariant is about
# the readers: however many buckets a query fans out into, none of them may touch the cache.
# The counts below are therefore asserted exactly rather than as booleans - a regressed gate
# shows up as one extra hit per bucket.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# 3200 rows at row-group size 50 => 64 row groups, well above the splitter's floor of 16 row
# groups per bucket, so at `max_threads = 8` the file fans out into several buckets. The file
# is far below the default `input_format_parquet_min_bytes_to_split`, so the split queries set
# that and `input_format_parquet_bytes_per_split_bucket` to 0 to opt out of the size-based gate.
#
# The multi-row-group file is staged through the `file` table function and the `File`-engine
# table is created over that path: an `INSERT` into the table itself would go through the
# engine's own write pipeline, which does not apply the query-level
# `output_format_parquet_row_group_size` and produces a single row group - nothing to split or
# prune. A `File`-engine table (rather than the `file` table function) is also what makes the
# query condition cache observable: a table function carries a nil storage UUID, for which
# `QueryConditionCache::{read,write}` are no-ops.
TAG="05136_${CLICKHOUSE_DATABASE}"
DATA_FILE_RELATIVE="${CLICKHOUSE_TEST_UNIQUE_NAME}/05136.parquet"

SPLIT_SETTINGS="use_parquet_metadata_cache = 1, parallelize_output_from_storages = 1, max_threads = 8, input_format_parquet_min_bytes_to_split = 0, input_format_parquet_bytes_per_split_bucket = 0"
PLAIN_SETTINGS="use_parquet_metadata_cache = 1, parallelize_output_from_storages = 1, max_threads = 1"

${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION file('${DATA_FILE_RELATIVE}') SELECT number FROM numbers(3200) SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_row_group_size = 50"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_05136"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_05136 (number UInt64) ENGINE = File(Parquet, '${DATA_FILE_RELATIVE}')"

# The query condition cache is bypassed until the file's version token has settled - the last
# modification must be comfortably in the past for the token to prove a later rewrite
# (`file_version_settle_seconds = 3` in `isFileCacheVersionSettled`) - so give the file time
# to settle. The format metadata cache is deliberately not gated on that, so this only matters
# for the two query-condition-cache queries at the end.
sleep 4

${CLICKHOUSE_CLIENT} --query "SYSTEM DROP PARQUET METADATA CACHE"

# The predicate matches a single value in the last row group (3175 lands in row group 63), so
# the query condition cache can prune all but one row group once it is populated.

# 1. Cold split read: the file's footer is not cached, so the split decision parses it and
# records exactly one miss. Each per-bucket source then parses the footer of its own open, so
# no hit is recorded even though the decision has just warmed the entry.
${CLICKHOUSE_CLIENT} --query "SELECT number FROM t_05136 WHERE number = 3175 SETTINGS ${SPLIT_SETTINGS}, use_query_condition_cache = 0, log_comment = '${TAG}_split_cold'"

# 2. Warm split read: the split decision is served by the cache-only fast path (which does not
# report hits or misses), and the per-bucket sources still bypass the cache, so the whole query
# records nothing at all.
${CLICKHOUSE_CLIENT} --query "SELECT number FROM t_05136 WHERE number = 3175 SETTINGS ${SPLIT_SETTINGS}, use_query_condition_cache = 0, log_comment = '${TAG}_split_warm'"

# 3. Plain unrestricted read of the same file: with no restriction to cross-validate there is
# nothing to fail close on, so the footer cache is used and this hits the entry warmed by (1).
# This is what makes the zeros above meaningful: the entry really is present and usable for
# this file, the split reads just must not use it.
${CLICKHOUSE_CLIENT} --query "SELECT number FROM t_05136 WHERE number = 3175 SETTINGS ${PLAIN_SETTINGS}, use_query_condition_cache = 0, log_comment = '${TAG}_plain'"

# 4. Plain read with the query condition cache on: the lookup misses, and the read stores the
# matching row groups together with the digest of the footer they were computed from. Still no
# restriction in place, so the footer cache is used again (one hit).
${CLICKHOUSE_CLIENT} --query "SELECT number FROM t_05136 WHERE number = 3175 SETTINGS ${PLAIN_SETTINGS}, use_query_condition_cache = 1, log_comment = '${TAG}_qcc_fill'"

# 5. Plain repeat: the query condition cache now hits and its marks become a row-group
# restriction (`buckets_to_read`), so this read must bypass the footer cache entirely - zero
# hits and zero misses - even though the previous query against the very same file hit it. The
# `qcc_hit` column proves the restriction really was built, so the zeros are not vacuous.
${CLICKHOUSE_CLIENT} --query "SELECT number FROM t_05136 WHERE number = 3175 SETTINGS ${PLAIN_SETTINGS}, use_query_condition_cache = 1, log_comment = '${TAG}_qcc_pruned'"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

# `EngineFileLikeReadFiles` counts one per source that opened the file: > 1 proves the split
# queries really were fanned out (so their zero cache accesses are about the bucketed path and
# not about an unsplit fallback read).
${CLICKHOUSE_CLIENT} --query "
    SELECT
        replaceOne(log_comment, '${TAG}_', ''),
        ProfileEvents['ParquetMetadataCacheHits'] AS footer_hits,
        ProfileEvents['ParquetMetadataCacheMisses'] AS footer_misses,
        ProfileEvents['QueryConditionCacheHits'] > 0 AS qcc_hit,
        ProfileEvents['EngineFileLikeReadFiles'] > 1 AS split
    FROM system.query_log
    WHERE current_database = currentDatabase()
        AND type = 'QueryFinish'
        AND query_kind = 'Select'
        AND log_comment LIKE '${TAG}_%'
    ORDER BY event_time_microseconds"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05136"
rm -r "${CLICKHOUSE_USER_FILES_UNIQUE:?}"
