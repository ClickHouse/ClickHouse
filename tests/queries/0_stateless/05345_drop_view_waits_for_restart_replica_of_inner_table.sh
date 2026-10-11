#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-fasttest, no-replicated-database
# Tag no-parallel: the failpoint pauses any `SYSTEM RESTART REPLICA` on the server.
# Tag no-replicated-database: the failpoint is enabled on one server only.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Drops table $2 while `SYSTEM RESTART REPLICA` holds its inner table $1 detached, then counts the inner tables left.
function drop_during_restart()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT restart_replica_pause_after_detach"
    $CLICKHOUSE_CLIENT -q "SYSTEM RESTART REPLICA \`$1\`" &
    local restart_pid=$!
    $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT restart_replica_pause_after_detach PAUSE"

    $CLICKHOUSE_CLIENT -q "DROP TABLE $2" &
    local drop_pid=$!
    # A DROP that does not wait for the restart finishes here, and the restart attaches the inner table back after it.
    for _ in {1..30}; do kill -0 $drop_pid 2>/dev/null || break; sleep 0.1; done

    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT restart_replica_pause_after_detach"
    wait $restart_pid $drop_pid
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner%'"
}

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE src (x Int64) ENGINE = MergeTree ORDER BY x;
    CREATE MATERIALIZED VIEW mv ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/mv', 'r1') ORDER BY x
        AS SELECT x FROM src;
"
drop_during_restart "$($CLICKHOUSE_CLIENT -q "SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.%'")" mv

$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -q "
    CREATE TABLE ts ENGINE = TimeSeries
        SAMPLES ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/ts', 'r1') ORDER BY (id, timestamp);
"
drop_during_restart "$($CLICKHOUSE_CLIENT -q "SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.samples.%'")" ts
