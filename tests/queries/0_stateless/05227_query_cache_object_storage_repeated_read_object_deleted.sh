#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3

# Regression test for `query_cache_use_only_when_data_was_not_changed` over an object-storage (S3)
# table that one query reads more than once while its object set changes between the reads. See
# PR #108721.
#
# The read records the object set it consumed so that the finalization check hashes exactly what was
# read instead of re-listing. The reads of one table used to be captured into a single union: the
# pre-read hash listed `{a, b}`, the first read consumed `{a, b}`, `b` was deleted, the second read
# consumed `{a}`, and the union `{a, b}` still matched the pre-read hash - so a result assembled from
# two states of the table was stored as if it came from one. The reads are now captured apart, and the
# consistency check fails closed when they disagree.
#
# The two reads are scalar subqueries, which execute one after the other, and a pauseable failpoint
# holds the second read of the table until the object has been deleted.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Object names are unique per run so concurrent runs (e.g. the flaky check) do not collide on the
# shared S3 bucket. The cache key includes the current database, so the query cache is isolated too.
prefix="test_05227_${CLICKHOUSE_DATABASE}"
table="${CLICKHOUSE_DATABASE}.t_s3_qc_repeated"
failpoint="object_storage_pause_before_repeated_read"

# Pin every query-cache setting so the flaky check's settings randomizer cannot change the outcome.
qc="use_query_cache = 1, enable_reads_from_query_cache = 1, enable_writes_to_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1"

${CLICKHOUSE_CLIENT} -q "INSERT INTO FUNCTION s3(s3_conn, filename = '${prefix}_1', format = 'TSV', structure = 'x UInt64') SELECT 10 SETTINGS s3_truncate_on_insert = 1"
${CLICKHOUSE_CLIENT} -q "INSERT INTO FUNCTION s3(s3_conn, filename = '${prefix}_2', format = 'TSV', structure = 'x UInt64') SELECT 20 SETTINGS s3_truncate_on_insert = 1"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${table}"
# An S3-engine table has a UUID (unlike the s3 table function), so it can report a modification hash.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${table} (x UInt64) ENGINE = S3(s3_conn, filename = '${prefix}_*', format = 'TSV')"

# Positive control: two reads of the unchanged table agree, so the query is stored on the first run and
# served from the cache on the second.
echo 'unchanged table read twice'
${CLICKHOUSE_CLIENT} -q "SELECT (SELECT sum(x) FROM ${table}) AS s, (SELECT count() FROM ${table}) AS c SETTINGS ${qc}"
${CLICKHOUSE_CLIENT} -q "SELECT (SELECT sum(x) FROM ${table}) AS s, (SELECT count() FROM ${table}) AS c SETTINGS ${qc}"

# The object set changes between the two reads: the first read consumes both objects, then one of them
# is deleted while the second read is held at the failpoint. The result (the sum of both objects next
# to a count of one) was built from two states of the table, so it must not be stored.
echo 'object deleted between two reads'
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT ${failpoint}"
${CLICKHOUSE_CLIENT} -q "SELECT (SELECT sum(x) FROM ${table}) AS sum_before, (SELECT count() FROM ${table}) AS count_after SETTINGS ${qc}" &
query_pid=$!
${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT ${failpoint} PAUSE"
# The S3 endpoint of `s3_conn` (see `tests/config/config.d/named_collection.xml`); the test bucket
# grants anonymous writes.
${CLICKHOUSE_CURL} -sS -X DELETE "http://localhost:11111/test/${prefix}_2"
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT ${failpoint}"
wait "${query_pid}"
${CLICKHOUSE_CLIENT} -q "SELECT 'stored entries', count() FROM system.query_cache WHERE query LIKE '%${table}%count_after%'"

# Cache hits per run: the unchanged reads hit only on the second run (0, 1); the mixed-state read does
# not hit (0).
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} -q "
SELECT ProfileEvents['QueryCacheHits']
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 600 SECOND AND type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND query LIKE 'SELECT (SELECT sum(x) FROM ${table})%'
ORDER BY event_time_microseconds"

${CLICKHOUSE_CLIENT} -q "DROP TABLE ${table}"
