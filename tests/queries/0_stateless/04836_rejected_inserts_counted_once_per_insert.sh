#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A plain INSERT fans out to `max_insert_threads` parallel sinks, and every sink evaluates the
# "too many parts" check while the insert chain is being built. One rejected INSERT must still be
# counted exactly once in the `RejectedInserts` profile event, not once per sink stream.

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_rejected_inserts (x UInt64) ENGINE = MergeTree ORDER BY x
    SETTINGS parts_to_throw_insert = 1, parts_to_delay_insert = 0, max_avg_part_size_for_too_many_parts = 0;
"

# Brings the table to the throw threshold.
$CLICKHOUSE_CLIENT --async_insert 0 --query "INSERT INTO t_rejected_inserts VALUES (1)"

QUERY_ID="04836_${CLICKHOUSE_DATABASE}_rejected"

$CLICKHOUSE_CLIENT --query_id "$QUERY_ID" --async_insert 0 --max_insert_threads 16 \
    --query "INSERT INTO t_rejected_inserts VALUES (2)" 2>&1 | grep -o -m1 'TOO_MANY_PARTS'

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"

$CLICKHOUSE_CLIENT --query "
    SELECT ProfileEvents['RejectedInserts']
    FROM system.query_log
    WHERE current_database = currentDatabase() AND query_id = '$QUERY_ID' AND type = 'ExceptionWhileProcessing'
    ORDER BY event_time_microseconds DESC
    LIMIT 1;
"

# Concurrent rejected inserts must each be counted once - the "already counted" state is
# per query, so one query's rejection must not be masked or double-counted by another.

for i in 1 2 3 4; do
    $CLICKHOUSE_CLIENT --query_id "${QUERY_ID}_concurrent_$i" --async_insert 0 --max_insert_threads 16 \
        --query "INSERT INTO t_rejected_inserts VALUES ($i)" 2>/dev/null &
done
wait

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"

# Grouping by `query_id` keeps the check stable if the test is rerun with the same query ids.
$CLICKHOUSE_CLIENT --query "
    SELECT count(), min(rejected), max(rejected)
    FROM
    (
        SELECT query_id, max(ProfileEvents['RejectedInserts']) AS rejected
        FROM system.query_log
        WHERE current_database = currentDatabase() AND query_id LIKE '${QUERY_ID}_concurrent_%' AND type = 'ExceptionWhileProcessing'
        GROUP BY query_id
    );
"

# One query can genuinely reject the insert into more than one table: its materialized views run
# under the same query, and with materialized_views_ignore_errors every full view target rejects
# the insert independently while the query itself succeeds. Every rejecting table must be counted,
# and each of them exactly once.

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_rejected_src (x UInt64) ENGINE = MergeTree ORDER BY x;

    CREATE TABLE t_rejected_dst_1 (x UInt64) ENGINE = MergeTree ORDER BY x
        SETTINGS parts_to_throw_insert = 1, parts_to_delay_insert = 0, max_avg_part_size_for_too_many_parts = 0;

    CREATE TABLE t_rejected_dst_2 (x UInt64) ENGINE = MergeTree ORDER BY x
        SETTINGS parts_to_throw_insert = 1, parts_to_delay_insert = 0, max_avg_part_size_for_too_many_parts = 0;

    CREATE MATERIALIZED VIEW t_rejected_mv_1 TO t_rejected_dst_1 AS SELECT x FROM t_rejected_src;
    CREATE MATERIALIZED VIEW t_rejected_mv_2 TO t_rejected_dst_2 AS SELECT x FROM t_rejected_src;
"

# Brings both view targets to the throw threshold.
$CLICKHOUSE_CLIENT --async_insert 0 --query "INSERT INTO t_rejected_dst_1 VALUES (1)"
$CLICKHOUSE_CLIENT --async_insert 0 --query "INSERT INTO t_rejected_dst_2 VALUES (1)"

# The ignored view errors are streamed to the client as warning logs; `send_logs_level` silences them.
$CLICKHOUSE_CLIENT --query_id "${QUERY_ID}_views" --async_insert 0 --max_insert_threads 16 \
    --query "INSERT INTO t_rejected_src SETTINGS materialized_views_ignore_errors = 1, send_logs_level = 'fatal' VALUES (2)"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"

$CLICKHOUSE_CLIENT --query "
    SELECT ProfileEvents['RejectedInserts']
    FROM system.query_log
    WHERE current_database = currentDatabase() AND query_id = '${QUERY_ID}_views' AND type = 'QueryFinish'
    ORDER BY event_time_microseconds DESC
    LIMIT 1;
"
