#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-fasttest, no-replicated-database
# Tag no-parallel: the failpoint pauses any `SYSTEM RESTART REPLICA` on the server.
# Tag no-replicated-database: the failpoint is enabled on one server only.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Drops table $1.$3 while `SYSTEM RESTART REPLICA` holds its inner table $1.$2 detached, then counts the inner tables left in $1.
function drop_during_restart()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT restart_replica_pause_after_detach"
    $CLICKHOUSE_CLIENT -q "SYSTEM RESTART REPLICA $1.\`$2\`" &
    local restart_pid=$!
    $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT restart_replica_pause_after_detach PAUSE"

    $CLICKHOUSE_CLIENT -q "DROP TABLE $1.$3" &
    local drop_pid=$!
    # A DROP that does not wait for the restart finishes here, and the restart attaches the inner table back after it.
    for _ in {1..30}; do kill -0 $drop_pid 2>/dev/null || break; sleep 0.1; done

    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT restart_replica_pause_after_detach"
    wait $restart_pid $drop_pid
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.tables WHERE database = '$1' AND name LIKE '.inner%'"
}

function inner_table_name()
{
    $CLICKHOUSE_CLIENT -q "SELECT name FROM system.tables WHERE database = '$1' AND name LIKE '$2'"
}

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE src (x Int64) ENGINE = MergeTree ORDER BY x;
    CREATE MATERIALIZED VIEW mv ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/mv', 'r1') ORDER BY x
        AS SELECT x FROM src;
"
drop_during_restart "$CLICKHOUSE_DATABASE" "$(inner_table_name "$CLICKHOUSE_DATABASE" '.inner_id.%')" mv

$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -q "
    CREATE TABLE ts ENGINE = TimeSeries
        SAMPLES ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/ts', 'r1') ORDER BY (id, timestamp);
"
drop_during_restart "$CLICKHOUSE_DATABASE" "$(inner_table_name "$CLICKHOUSE_DATABASE" '.inner_id.samples.%')" ts

# A `Memory` database drops tables synchronously, without the drop task of an `Atomic` one.
MEMORY_DATABASE="${CLICKHOUSE_DATABASE}_memory"
$CLICKHOUSE_CLIENT -q "
    CREATE DATABASE $MEMORY_DATABASE ENGINE = Memory;
    CREATE MATERIALIZED VIEW $MEMORY_DATABASE.mv ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/mv_memory', 'r1') ORDER BY x
        AS SELECT x FROM $CLICKHOUSE_DATABASE.src;
"
drop_during_restart "$MEMORY_DATABASE" "$(inner_table_name "$MEMORY_DATABASE" '.inner%')" mv
$CLICKHOUSE_CLIENT -q "DROP DATABASE $MEMORY_DATABASE"
