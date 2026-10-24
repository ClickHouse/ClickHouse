#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3

# Regression test for `query_cache_use_only_when_data_was_not_changed` over an object-storage (S3)
# table. `StorageObjectStorage::getModificationHash` lists the objects behind the table independently
# of the read, so the finalization consistency check could re-list the same object set even though the
# read consumed a different one. To close that, the read records the object set it actually consumed
# (`QueryConsumedObjectSets`) and `getModificationHash` hashes that set at finalization. The cache is
# then populated only while the consumed set matches the object set folded into its key (so the second
# run below is a cache hit), and invalidated when an object changes. See PR #108721 (review 3508863459).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Object names are unique per run so concurrent runs (e.g. the flaky check) do not collide on the
# shared S3 bucket. The cache key includes the current database, so the query cache is isolated too.
prefix="test_04495_${CLICKHOUSE_DATABASE}"

# Pin every query-cache setting so the flaky check's settings randomizer cannot change the outcome.
qc="use_query_cache = 1, enable_reads_from_query_cache = 1, enable_writes_to_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1"

# Fixed object names, always overwritten, so the read set is exactly {1, 2}.
${CLICKHOUSE_CLIENT} -q "INSERT INTO FUNCTION s3(s3_conn, filename = '${prefix}_1', format = 'TSV', structure = 'x UInt64') SELECT 10 SETTINGS s3_truncate_on_insert = 1"
${CLICKHOUSE_CLIENT} -q "INSERT INTO FUNCTION s3(s3_conn, filename = '${prefix}_2', format = 'TSV', structure = 'x UInt64') SELECT 20 SETTINGS s3_truncate_on_insert = 1"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_s3_qc"
# An S3-engine table has a UUID (unlike the s3 table function), so it can report a modification hash.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_s3_qc (x UInt64) ENGINE = S3(s3_conn, filename = '${prefix}_*', format = 'TSV')"

# First run stores the result (the read-consumed object set matches the pre-read listing); the second
# run over the unchanged object set is served from the cache.
${CLICKHOUSE_CLIENT} -q "SELECT sum(x) FROM t_s3_qc SETTINGS ${qc}"
${CLICKHOUSE_CLIENT} -q "SELECT sum(x) FROM t_s3_qc SETTINGS ${qc}"

# Rewrite one object (new content, new ETag): the cache is invalidated and the result recomputed.
${CLICKHOUSE_CLIENT} -q "INSERT INTO FUNCTION s3(s3_conn, filename = '${prefix}_2', format = 'TSV', structure = 'x UInt64') SELECT number + 21 FROM numbers(3) SETTINGS s3_truncate_on_insert = 1"
${CLICKHOUSE_CLIENT} -q "SELECT sum(x) FROM t_s3_qc SETTINGS ${qc}"

# The second run must be a cache hit (1); the first run and the post-rewrite run must not (0). The hit
# only happens when the consumed-set hash matches the pre-read cache key.
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} -q "
SELECT ProfileEvents['QueryCacheHits']
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 600 SECOND AND type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND query LIKE 'SELECT sum(x) FROM t_s3_qc%'
ORDER BY event_time_microseconds"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_s3_qc"
