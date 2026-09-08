#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the async insert queue and an explicit flush.

# An async insert queued by one user and flushed by another must keep its accounting
# separate from the flushing user: the flushed insert is a query of its own (attributed to
# the inserting user), and its profile events are rolled up into the enclosing
# `SYSTEM FLUSH ASYNC INSERT QUEUE` query locally, without linking the flush query's
# counters to the inserting user's counter chain.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user_insert="u_insert_${CLICKHOUSE_DATABASE}"
user_flush="u_flush_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
DROP USER IF EXISTS $user_insert, $user_flush;
CREATE USER $user_insert IDENTIFIED WITH no_password;
CREATE USER $user_flush IDENTIFIED WITH no_password;
CREATE TABLE t_cross_user_flush (x UInt64) ENGINE = MergeTree ORDER BY x;
GRANT INSERT ON ${CLICKHOUSE_DATABASE}.t_cross_user_flush TO $user_insert;
GRANT SYSTEM FLUSH ASYNC INSERT QUEUE ON *.* TO $user_flush;
GRANT SHOW TABLES ON ${CLICKHOUSE_DATABASE}.* TO $user_flush;
"

# Long busy timeout so nothing fires on its own; the explicit flush below does the work.
async_insert=(--async_insert=1 --wait_for_async_insert=0
    --async_insert_busy_timeout_min_ms=600000 --async_insert_busy_timeout_max_ms=600000
    --async_insert_use_adaptive_busy_timeout=0)

$CLICKHOUSE_CLIENT --user "$user_insert" "${async_insert[@]}" \
    -q "INSERT INTO t_cross_user_flush VALUES (1), (2)"

flush_query_id="${CLICKHOUSE_DATABASE}_cross_user_flush_$RANDOM"
$CLICKHOUSE_CLIENT --user "$user_flush" --query_id "$flush_query_id" \
    -q "SYSTEM FLUSH ASYNC INSERT QUEUE ${CLICKHOUSE_DATABASE}.t_cross_user_flush"

$CLICKHOUSE_CLIENT -q "SELECT 'rows', count() FROM t_cross_user_flush"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log, asynchronous_insert_log"

# The insert executed by the flush is a query of its own: it must stay attributed to the
# user that queued it, not to the user that ran the flush.
$CLICKHOUSE_CLIENT -q "
WITH (
    SELECT flush_query_id
    FROM system.asynchronous_insert_log
    WHERE event_date >= yesterday() AND database = currentDatabase() AND table = 't_cross_user_flush'
    ORDER BY event_time_microseconds DESC LIMIT 1
) AS flushed_insert_query_id
SELECT
    'flushed insert attributed to the inserting user', user = '$user_insert',
    'flushed insert wrote rows', ProfileEvents['InsertedRows'] = 2
FROM system.query_log
WHERE event_date >= yesterday() AND query_id = flushed_insert_query_id AND type = 'QueryFinish'
"

# The flush query keeps its own user and still accounts the insert it executed,
# because the insert counters are rolled up into it locally.
$CLICKHOUSE_CLIENT -q "
SELECT
    'flush attributed to the flushing user', user = '$user_flush',
    'flush rolled up the insert counters', ProfileEvents['InsertedRows'] = 2
FROM system.query_log
WHERE event_date >= yesterday()
  AND query_id = '$flush_query_id'
  AND type = 'QueryFinish'
"

$CLICKHOUSE_CLIENT -q "DROP USER $user_insert, $user_flush"
