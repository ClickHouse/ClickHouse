#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3

# Regression test for the modification hash of an object-storage table that applies metadata lazily.
# The first use of such a table resolves the deferred Hive-partitioning sample path through
# `setInMemoryMetadata`, which advances the metadata version folded into the hash. Every path that
# hashes a table directly - the `Merge` and local `Distributed` wrappers and the
# `system.tables.modification_hash` column - must refresh that metadata before hashing
# (`getModificationHashWithRefreshedMetadata`); otherwise the hash taken before the first read differs
# from the one taken after it, and `query_cache_use_only_when_data_was_not_changed` deterministically
# fails to cache the first query through a wrapper. See PR #108721.
#
# The `URL('s3://...')` scheme dispatch takes the same path but is not covered here: a bare `s3://` URL
# carries no endpoint reachable from the test environment (see 04640).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Object names are unique per run so concurrent runs (e.g. the flaky check) do not collide on the
# shared S3 bucket. The cache key includes the current database, so the query cache is isolated too.
prefix="test_04812_${CLICKHOUSE_DATABASE}"

# Pin every query-cache setting so the flaky check's settings randomizer cannot change the outcome.
qc="use_query_cache = 1, enable_reads_from_query_cache = 1, enable_writes_to_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1"

${CLICKHOUSE_CLIENT} -q "INSERT INTO FUNCTION s3(s3_conn, filename = '${prefix}_plain', format = 'TSV', structure = 'x UInt64') SELECT 10 SETTINGS s3_truncate_on_insert = 1"

# An S3-engine table has a UUID (unlike the s3 table function), so it can report a modification hash.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_s3_04812 (x UInt64) ENGINE = S3(s3_conn, filename = '${prefix}_plain', format = 'TSV')"

# The `system.tables.modification_hash` of a table that has never been read must already reflect the
# resolved metadata, so an ordinary read of the table does not change it.
${CLICKHOUSE_CLIENT} -q "
CREATE TABLE hashes_04812 (name String, hash UInt128) ENGINE = Memory;
INSERT INTO hashes_04812 SELECT 'before', modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 't_s3_04812';"
${CLICKHOUSE_CLIENT} -q "SELECT sum(x) FROM t_s3_04812"
${CLICKHOUSE_CLIENT} -q "
INSERT INTO hashes_04812 SELECT 'after', modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 't_s3_04812';
SELECT 'hash unchanged by the first read', (SELECT hash FROM hashes_04812 WHERE name = 'before') = (SELECT hash FROM hashes_04812 WHERE name = 'after');"

# Each wrapper reads a freshly created object-storage table, so its very first consistent-hash query is
# the one that used to be affected: the pre-read hash of the child was taken before the child's own
# first-use metadata update, so the finalization check saw a different value and nothing was cached.
for wrapper in merge dist
do
    ${CLICKHOUSE_CLIENT} -q "INSERT INTO FUNCTION s3(s3_conn, filename = '${prefix}_${wrapper}', format = 'TSV', structure = 'x UInt64') SELECT 30 SETTINGS s3_truncate_on_insert = 1"
    ${CLICKHOUSE_CLIENT} -q "CREATE TABLE child_${wrapper}_04812 (x UInt64) ENGINE = S3(s3_conn, filename = '${prefix}_${wrapper}', format = 'TSV')"
done

${CLICKHOUSE_CLIENT} -q "CREATE TABLE merge_04812 (x UInt64) ENGINE = Merge(currentDatabase(), '^child_merge_04812\$')"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE dist_04812 AS child_dist_04812 ENGINE = Distributed(test_shard_localhost, currentDatabase(), child_dist_04812)"

# The first run stores the result, the second run over the unchanged object set is served from the cache.
for wrapper in merge dist
do
    ${CLICKHOUSE_CLIENT} -q "SELECT sum(x) FROM ${wrapper}_04812 SETTINGS ${qc}"
    ${CLICKHOUSE_CLIENT} -q "SELECT sum(x) FROM ${wrapper}_04812 SETTINGS ${qc}"
done

# For each wrapper: the first run must not be a cache hit (0), the second must be (1).
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
for wrapper in merge dist
do
    ${CLICKHOUSE_CLIENT} -q "
    SELECT '${wrapper}', ProfileEvents['QueryCacheHits']
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 600 SECOND AND type = 'QueryFinish'
      AND current_database = currentDatabase()
      AND query LIKE 'SELECT sum(x) FROM ${wrapper}_04812%'
    ORDER BY event_time_microseconds"
done

${CLICKHOUSE_CLIENT} -q "DROP TABLE merge_04812, dist_04812, child_merge_04812, child_dist_04812, t_s3_04812, hashes_04812"
