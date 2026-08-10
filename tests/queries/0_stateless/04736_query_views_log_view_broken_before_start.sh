#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

dangling="${CLICKHOUSE_DATABASE}_dangling_definer"
present="${CLICKHOUSE_DATABASE}_present_definer"

# Both views fail on purpose, and the server logs each failure at warning level,
# which the client forwards to stderr and clickhouse-test treats as a failure.
$CLICKHOUSE_CLIENT --send_logs_level=error -nm -q "
DROP USER IF EXISTS ${dangling}, ${dangling}_gone, ${present};

CREATE USER ${dangling};
GRANT SELECT, INSERT ON *.* TO ${dangling};

-- Resolves, but cannot insert, so this view fails only after its select context
-- has been built.
CREATE USER ${present};
GRANT SELECT ON *.* TO ${present};

CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE dst_dangling (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE dst_present (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE dst_ok (x UInt64) ENGINE = MergeTree ORDER BY x;

CREATE MATERIALIZED VIEW mv_dangling TO dst_dangling
DEFINER = ${dangling} SQL SECURITY DEFINER AS SELECT x FROM src;

CREATE MATERIALIZED VIEW mv_present TO dst_present
DEFINER = ${present} SQL SECURITY DEFINER AS SELECT x FROM src;

CREATE MATERIALIZED VIEW mv_ok TO dst_ok AS SELECT x FROM src;

-- Renaming the definer leaves mv_dangling's stored DEFINER name unresolvable, so
-- building its select context throws while its bookkeeping is only half filled.
ALTER USER ${dangling} RENAME TO ${dangling}_gone;

INSERT INTO src SETTINGS materialized_views_ignore_errors = 1, log_queries = 1, log_query_views = 1 VALUES (1);

SYSTEM FLUSH LOGS query_views_log;
"

# The half-built view must still produce exactly one row, and it is the only one
# whose logged query is empty. Before the fix the server aborted while logging it.
$CLICKHOUSE_CLIENT -q "
SELECT
    substring(view_name, length(currentDatabase()) + 2) AS view,
    status,
    view_query = '' AS view_query_is_empty,
    count()
FROM system.query_views_log
WHERE event_date >= yesterday()
  AND event_time >= now() - INTERVAL 5 MINUTE
  AND view_name IN (
      concatWithSeparator('.', currentDatabase(), 'mv_dangling'),
      concatWithSeparator('.', currentDatabase(), 'mv_present'),
      concatWithSeparator('.', currentDatabase(), 'mv_ok'))
GROUP BY view, status, view_query_is_empty
ORDER BY view
"

$CLICKHOUSE_CLIENT -q "SELECT 'rows in the succeeding view target', count() FROM dst_ok"

# The views must go before the users: a user that is still a definer of a live view
# cannot be dropped. Both views are dropped synchronously first for that reason.
$CLICKHOUSE_CLIENT -nm -q "
DROP TABLE mv_dangling SYNC;
DROP TABLE mv_present SYNC;
DROP USER IF EXISTS ${dangling}, ${dangling}_gone, ${present};
"
