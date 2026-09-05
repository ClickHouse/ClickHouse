#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3

# Regression test for `query_cache_use_only_when_data_was_not_changed` over an object-storage (S3)
# table when the read consumes no object at all. See PR #108721.
#
# The read records the object set it consumed so that the finalization check hashes exactly what was
# read instead of re-listing. A read that consumes zero objects used to record nothing, which is
# indistinguishable from "this table was not read": the finalization check then fell back to a fresh
# listing, reproduced the pre-read hash and stored a result that was produced from no data at all
# under the key of the full object set. The read now records an empty consumed set up front, so the
# two hashes differ and such a query is not cached.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Object names are unique per run so concurrent runs (e.g. the flaky check) do not collide on the
# shared S3 bucket.
prefix="test_04846_${CLICKHOUSE_DATABASE}"

# Pin every query-cache setting so the flaky check's settings randomizer cannot change the outcome.
qc_common="use_query_cache = 1, enable_reads_from_query_cache = 1, enable_writes_to_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0"
qc="${qc_common}, query_cache_use_only_when_data_was_not_changed = 1"
qc_off="${qc_common}, query_cache_use_only_when_data_was_not_changed = 0"

${CLICKHOUSE_CLIENT} -q "INSERT INTO FUNCTION s3(s3_conn, filename = '${prefix}_1', format = 'TSV', structure = 'x UInt64') SELECT 10 SETTINGS s3_truncate_on_insert = 1"
${CLICKHOUSE_CLIENT} -q "INSERT INTO FUNCTION s3(s3_conn, filename = '${prefix}_2', format = 'TSV', structure = 'x UInt64') SELECT 20 SETTINGS s3_truncate_on_insert = 1"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_s3_qc_zero"
# An S3-engine table has a UUID (unlike the s3 table function), so it can report a modification hash.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_s3_qc_zero (x UInt64) ENGINE = S3(s3_conn, filename = '${prefix}_*', format = 'TSV')"

# A full read of the unchanged table is stored and the second run is served from the cache. This is
# the positive control: the consistency check does not disable the cache for this table as such.
echo 'full read'
${CLICKHOUSE_CLIENT} -q "SELECT sum(x) FROM ${CLICKHOUSE_DATABASE}.t_s3_qc_zero SETTINGS ${qc}"
${CLICKHOUSE_CLIENT} -q "SELECT sum(x) FROM ${CLICKHOUSE_DATABASE}.t_s3_qc_zero SETTINGS ${qc}"

# A read that consumes no object while the table has objects cannot be validated against the pre-read
# hash of the full listing: the consistency check fails closed, so nothing is stored and no run hits.
echo 'zero-object read'
${CLICKHOUSE_CLIENT} -q "SELECT x FROM ${CLICKHOUSE_DATABASE}.t_s3_qc_zero ORDER BY x LIMIT 0 SETTINGS ${qc}"
${CLICKHOUSE_CLIENT} -q "SELECT x FROM ${CLICKHOUSE_DATABASE}.t_s3_qc_zero ORDER BY x LIMIT 0 SETTINGS ${qc}"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.query_cache WHERE query LIKE '%${CLICKHOUSE_DATABASE}.t_s3_qc_zero%LIMIT 0%'"

# Negative control: without the consistency check the very same query is cacheable, so the absence of
# an entry above is the consistency check failing closed and not the query being uncacheable.
echo 'zero-object read, consistency check off'
${CLICKHOUSE_CLIENT} -q "SELECT x FROM ${CLICKHOUSE_DATABASE}.t_s3_qc_zero WHERE x > 0 ORDER BY x LIMIT 0 SETTINGS ${qc_off}"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.query_cache WHERE query LIKE '%${CLICKHOUSE_DATABASE}.t_s3_qc_zero%x > 0%'"

# Cache hits per run: the full reads hit only on the second, unchanged run (0, 1); the zero-object
# reads never hit (0, 0).
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} -q "
SELECT ProfileEvents['QueryCacheHits']
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 600 SECOND AND type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND query LIKE 'SELECT %FROM ${CLICKHOUSE_DATABASE}.t_s3_qc_zero%'
  AND query NOT LIKE '%x > 0%'
ORDER BY event_time_microseconds"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_s3_qc_zero"
