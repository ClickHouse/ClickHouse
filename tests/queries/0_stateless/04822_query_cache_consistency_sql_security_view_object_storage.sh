#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3

# Regression test for `query_cache_use_only_when_data_was_not_changed` through a `SQL SECURITY DEFINER`
# (or `NONE`) view over object storage. The view's inner query runs under a fresh query context built by
# `getSQLSecurityOverriddenContext`; that context must share the outer query's `QueryConsumedObjectSets`
# capture, otherwise the inner object-storage read records nothing and the finalization consistency check
# silently falls back to re-listing - reopening the listing `A -> B -> A` race the capture closes. The
# deterministic observable used here is the pruned-read marker (same as 04545, but through a view): a
# `_file` predicate inside the view makes the read consume only a subset of the table's objects, so the
# capture must be marked pruned and the consistency check must fail closed - nothing may be stored in the
# query cache and no run may be a cache hit. Before the fix the marker landed in the discarded inner
# capture, an entry was (wrongly) stored and the second run was served from the cache. See PR #108721.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Object names are unique per run so concurrent runs (e.g. the flaky check) do not collide on the
# shared S3 bucket. The database name folded into the queries below keeps the stored query texts (and
# so the `system.query_cache` filtering) unique per run too.
prefix="test_04822_${CLICKHOUSE_DATABASE}"

# Pin every query-cache setting so the flaky check's settings randomizer cannot change the outcome.
qc="use_query_cache = 1, enable_reads_from_query_cache = 1, enable_writes_to_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1"

${CLICKHOUSE_CLIENT} -q "INSERT INTO FUNCTION s3(s3_conn, filename = '${prefix}_a', format = 'TSV', structure = 'x UInt64') SELECT 1 SETTINGS s3_truncate_on_insert = 1"
${CLICKHOUSE_CLIENT} -q "INSERT INTO FUNCTION s3(s3_conn, filename = '${prefix}_b', format = 'TSV', structure = 'x UInt64') SELECT 2 SETTINGS s3_truncate_on_insert = 1"

# An S3-engine table has a UUID (unlike the s3 table function), so it can report a modification hash.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_s3_04822 (x UInt64) ENGINE = S3(s3_conn, filename = '${prefix}_{a,b}', format = 'TSV')"

# A plain (unpruned) read through the view: the shared capture records the consumed object set, the
# first run stores the result and the second run over the unchanged object set is a cache hit.
${CLICKHOUSE_CLIENT} -q "CREATE VIEW v_plain_04822 SQL SECURITY DEFINER AS SELECT sum(x) AS s FROM t_s3_04822"
${CLICKHOUSE_CLIENT} -q "SELECT s FROM v_plain_04822 WHERE '${CLICKHOUSE_DATABASE}' != '' SETTINGS ${qc}"
${CLICKHOUSE_CLIENT} -q "SELECT s FROM v_plain_04822 WHERE '${CLICKHOUSE_DATABASE}' != '' SETTINGS ${qc}"

# A pruned read through the view must fail the consistency check closed: nothing is stored, so the
# second run cannot be a cache hit.
${CLICKHOUSE_CLIENT} -q "CREATE VIEW v_pruned_04822 SQL SECURITY DEFINER AS SELECT sum(x) AS s FROM t_s3_04822 WHERE _file = '${prefix}_a'"
${CLICKHOUSE_CLIENT} -q "SELECT s FROM v_pruned_04822 WHERE '${CLICKHOUSE_DATABASE}' != '' SETTINGS ${qc}"
${CLICKHOUSE_CLIENT} -q "SELECT s FROM v_pruned_04822 WHERE '${CLICKHOUSE_DATABASE}' != '' SETTINGS ${qc}"

# The same through a `SQL SECURITY NONE` view (the other branch that builds a fresh query context).
${CLICKHOUSE_CLIENT} -q "CREATE VIEW v_pruned_none_04822 SQL SECURITY NONE AS SELECT sum(x) AS s FROM t_s3_04822 WHERE _file = '${prefix}_a'"
${CLICKHOUSE_CLIENT} -q "SELECT s FROM v_pruned_none_04822 WHERE '${CLICKHOUSE_DATABASE}' != '' SETTINGS ${qc}"
${CLICKHOUSE_CLIENT} -q "SELECT s FROM v_pruned_none_04822 WHERE '${CLICKHOUSE_DATABASE}' != '' SETTINGS ${qc}"

# Cache hits per run: the plain view stores on the first run and hits on the second (0, 1); the pruned
# views must never store, so no run may hit (0, 0 each).
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
for view in v_plain_04822 v_pruned_04822 v_pruned_none_04822
do
    ${CLICKHOUSE_CLIENT} -q "
    SELECT '${view}', ProfileEvents['QueryCacheHits']
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 600 SECOND AND type = 'QueryFinish'
      AND current_database = currentDatabase()
      AND query LIKE 'SELECT s FROM ${view}%'
    ORDER BY event_time_microseconds"
done

# The pruned queries must not have stored anything (their texts embed the database name, so entries
# from concurrent runs of this test do not match the filter).
${CLICKHOUSE_CLIENT} -q "SELECT 'pruned stored entries', count() FROM system.query_cache WHERE query LIKE '%v_pruned%04822%' AND query LIKE '%${CLICKHOUSE_DATABASE}%'"

${CLICKHOUSE_CLIENT} -q "DROP TABLE v_plain_04822, v_pruned_04822, v_pruned_none_04822, t_s3_04822"
