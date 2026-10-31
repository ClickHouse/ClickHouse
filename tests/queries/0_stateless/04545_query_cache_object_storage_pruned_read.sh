#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3

# Regression test for `query_cache_use_only_when_data_was_not_changed` over an object-storage (S3)
# table when the read is pruned or the table is read more than once. See PR #108721.
#
# - A read pruned by a `_file`/`_path`/Hive-partition filter consumes only a subset of the table's
#   objects, which could never hash equal to the pre-read hash of the full listing. The consistency
#   check fails closed for such a read (the capture is marked pruned), so the cache is bypassed:
#   no run may be served from the cache, and every run must return the correct (pruned) result.
# - A query that reads the same table twice (e.g. `UNION ALL`) captures every object once per read.
#   Exact duplicates are collapsed before hashing, so the consumed set matches the pre-read listing
#   and the second unchanged run is served from the cache.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Object names are unique per run so concurrent runs (e.g. the flaky check) do not collide on the
# shared S3 bucket. The cache key includes the current database, so the query cache is isolated too.
prefix="test_04545_${CLICKHOUSE_DATABASE}"

# Pin every query-cache setting so the flaky check's settings randomizer cannot change the outcome.
qc="use_query_cache = 1, enable_reads_from_query_cache = 1, enable_writes_to_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1"

${CLICKHOUSE_CLIENT} -q "INSERT INTO FUNCTION s3(s3_conn, filename = '${prefix}_1', format = 'TSV', structure = 'x UInt64') SELECT 10 SETTINGS s3_truncate_on_insert = 1"
${CLICKHOUSE_CLIENT} -q "INSERT INTO FUNCTION s3(s3_conn, filename = '${prefix}_2', format = 'TSV', structure = 'x UInt64') SELECT 20 SETTINGS s3_truncate_on_insert = 1"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_s3_qc_pruned"
# An S3-engine table has a UUID (unlike the s3 table function), so it can report a modification hash.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_s3_qc_pruned (x UInt64) ENGINE = S3(s3_conn, filename = '${prefix}_*', format = 'TSV')"

# Pruned read: the `_file` filter narrows the object set to {1}, so the consistency check fails
# closed. Both runs return the pruned result and neither run is served from the cache.
${CLICKHOUSE_CLIENT} -q "SELECT sum(x) FROM t_s3_qc_pruned WHERE _file = '${prefix}_1' SETTINGS ${qc}"
${CLICKHOUSE_CLIENT} -q "SELECT sum(x) FROM t_s3_qc_pruned WHERE _file = '${prefix}_1' SETTINGS ${qc}"

# Two full reads of the same table: the captured set contains every object twice; deduplication makes
# it match the pre-read listing, so the second unchanged run is a cache hit.
${CLICKHOUSE_CLIENT} -q "SELECT sum(x) FROM (SELECT x FROM t_s3_qc_pruned UNION ALL SELECT x FROM t_s3_qc_pruned) SETTINGS ${qc}"
${CLICKHOUSE_CLIENT} -q "SELECT sum(x) FROM (SELECT x FROM t_s3_qc_pruned UNION ALL SELECT x FROM t_s3_qc_pruned) SETTINGS ${qc}"

# Rewrite one object (new content, new ETag): the cached double-read entry is invalidated.
${CLICKHOUSE_CLIENT} -q "INSERT INTO FUNCTION s3(s3_conn, filename = '${prefix}_2', format = 'TSV', structure = 'x UInt64') SELECT 21 SETTINGS s3_truncate_on_insert = 1"
${CLICKHOUSE_CLIENT} -q "SELECT sum(x) FROM (SELECT x FROM t_s3_qc_pruned UNION ALL SELECT x FROM t_s3_qc_pruned) SETTINGS ${qc}"

# Cache hits per run: the pruned runs must never hit (0, 0); the double-read runs hit only on the
# second, unchanged run (0, 1, 0).
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} -q "
SELECT ProfileEvents['QueryCacheHits']
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 600 SECOND AND type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND query LIKE 'SELECT sum(x) FROM %t_s3_qc_pruned%'
ORDER BY event_time_microseconds"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_s3_qc_pruned"
