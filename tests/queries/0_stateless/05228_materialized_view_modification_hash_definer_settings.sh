#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -u

# Regression test for `query_cache_use_only_when_data_was_not_changed` over a `SQL SECURITY DEFINER`
# materialized view (PR #108721). Reading such a view reads its target table under
# `getSQLSecurityOverriddenContext`, so the rows it returns depend on the *effective* reader's settings:
# the definer's profile plus the invoker's changes clamped to the definer's constraints. A definer
# profile edit that changes what the target read returns (a read limit here) must therefore move the
# view's modification hash, exactly as it does for a plain view - otherwise the consistency check kept
# serving the result cached before the edit.

# Users are server-wide, so make the name unique per run: concurrent runs (e.g. the flaky check) must
# not see each other's grants. The database name folded into the queries below keeps the stored query
# texts - and so the `system.query_log` filtering - unique per run too.
definer="definer_05228_${CLICKHOUSE_DATABASE}"

# Pin every query-cache setting so the flaky check's settings randomizer cannot change the outcome.
qc="use_query_cache = 1, enable_reads_from_query_cache = 1, enable_writes_to_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1"

$CLICKHOUSE_CLIENT -q "
    CREATE USER ${definer};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${definer};

    CREATE TABLE src_05228 (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE target_05228 (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO target_05228 VALUES (1), (2), (3);

    CREATE MATERIALIZED VIEW mv_05228 TO target_05228 (x UInt64)
        DEFINER = ${definer} SQL SECURITY DEFINER AS SELECT x FROM src_05228;
"

# Two runs of the same read: the second one is served from the cache (positive control).
$CLICKHOUSE_CLIENT -q "SELECT sum(x) FROM mv_05228 WHERE '${CLICKHOUSE_DATABASE}' != '' SETTINGS ${qc}"
$CLICKHOUSE_CLIENT -q "SELECT sum(x) FROM mv_05228 WHERE '${CLICKHOUSE_DATABASE}' != '' SETTINGS ${qc}"

# A read limit in the definer's profile changes what the target read does: with three rows to read it
# now fails. The third run must not be served the result cached before the edit; it has to run the
# read again under the new effective settings, and so fail.
# The limit is `CONST`: the invoker's changed settings are applied over the definer's profile, and the
# CI profile of the invoker sets `max_rows_to_read` itself, which would otherwise win. A constraint of
# the definer drops the invoker's change instead (`clampToSettingsConstraints`).
$CLICKHOUSE_CLIENT -q "ALTER USER ${definer} SETTINGS max_rows_to_read = 1 CONST"
$CLICKHOUSE_CLIENT -q "SELECT sum(x) FROM mv_05228 WHERE '${CLICKHOUSE_DATABASE}' != '' SETTINGS ${qc}" 2>&1 | grep -o -m1 'TOO_MANY_ROWS'

# Cache hits per run: the first run stores, the second hits, the third one misses (0, 1).
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
SELECT 'hits', groupArray(hits)
FROM
(
    SELECT ProfileEvents['QueryCacheHits'] AS hits
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 600 SECOND AND type = 'QueryFinish'
      AND current_database = currentDatabase()
      AND query LIKE 'SELECT sum(x) FROM mv_05228 WHERE%'
    ORDER BY event_time_microseconds
)"

# The same through the view's own modification hash, which is what the cache key folds in.
hash_of_view()
{
    $CLICKHOUSE_CLIENT -q "
        SELECT toString(modification_hash)
        FROM system.tables
        WHERE database = currentDatabase() AND name = 'mv_05228'
    "
}

baseline=$(hash_of_view)
[ -n "${baseline}" ] && echo 'hash is computed'

# A purely operational setting in the definer's profile changes no row of the target read.
$CLICKHOUSE_CLIENT -q "ALTER USER ${definer} SETTINGS max_rows_to_read = 1 CONST, log_queries = 1"
[ "${baseline}" = "$(hash_of_view)" ] && echo 'an operational setting in the definer profile does not change the hash'

# A read limit does.
$CLICKHOUSE_CLIENT -q "ALTER USER ${definer} SETTINGS max_rows_to_read = 2 CONST, log_queries = 1"
[ "${baseline}" != "$(hash_of_view)" ] && echo 'a read limit in the definer profile changes the hash'

# The view first: a user cannot be dropped while it is the definer of one.
$CLICKHOUSE_CLIENT -q "
    DROP TABLE mv_05228;
    DROP TABLE target_05228;
    DROP TABLE src_05228;
    DROP USER ${definer};
"
