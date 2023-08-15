#!/usr/bin/env bash
# Tags: race, zookeeper, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./replication.lib
. "$CURDIR"/replication.lib


$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS alter_table0;
    DROP TABLE IF EXISTS alter_table1;

    CREATE TABLE alter_table0 (a UInt8, b Int16) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/alter_table', 'r1') ORDER BY a;
    CREATE TABLE alter_table1 (a UInt8, b Int16) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/alter_table', 'r2') ORDER BY a;
" || exit 1

function thread_detach()
{
    while true; do
        $CLICKHOUSE_CLIENT -mn -q "ALTER TABLE alter_table$(($RANDOM % 2)) DETACH PARTITION ID 'all'; SELECT sleep($RANDOM / 32000) format Null;" 2>/dev/null ||:
    done
}
function thread_attach()
{
    while true; do
        $CLICKHOUSE_CLIENT -mn -q "ALTER TABLE alter_table$(($RANDOM % 2)) ATTACH PARTITION ID 'all'; SELECT sleep($RANDOM / 32000) format Null;" 2>/dev/null ||:
    done
}

function insert()
{
    $CLICKHOUSE_CLIENT -q "INSERT INTO alter_table$(($RANDOM % 2)) SELECT $RANDOM, $i" 2>/dev/null
}

thread_detach & PID_1=$!
thread_attach & PID_2=$!
thread_detach & PID_3=$!
thread_attach & PID_4=$!

function do_inserts()
{
    for i in {1..20}; do
        while ! insert; do $CLICKHOUSE_CLIENT -q "SELECT '$CLICKHOUSE_DATABASE', 'retrying insert $i' FORMAT Null"; done
    done
}

$CLICKHOUSE_CLIENT -q "SELECT '$CLICKHOUSE_DATABASE', 'begin inserts'"
do_inserts 2>&1| grep -Fa "Exception: " | grep -Fv "was cancelled by concurrent ALTER PARTITION"
$CLICKHOUSE_CLIENT -q "SELECT '$CLICKHOUSE_DATABASE', 'end inserts'"

kill -TERM $PID_1 && kill -TERM $PID_2 && kill -TERM $PID_3 && kill -TERM $PID_4
wait

$CLICKHOUSE_CLIENT -q "SYSTEM SYNC REPLICA alter_table0"
$CLICKHOUSE_CLIENT -q "SYSTEM SYNC REPLICA alter_table1"
query_with_retry "ALTER TABLE alter_table0 ATTACH PARTITION ID 'all'" 2>/dev/null;
$CLICKHOUSE_CLIENT -q "ALTER TABLE alter_table1 ATTACH PARTITION ID 'all'" 2>/dev/null
$CLICKHOUSE_CLIENT -q "SYSTEM SYNC REPLICA alter_table1"
$CLICKHOUSE_CLIENT -q "ALTER TABLE alter_table1 ATTACH PARTITION ID 'all'"
$CLICKHOUSE_CLIENT -q "SYSTEM SYNC REPLICA alter_table0"
$CLICKHOUSE_CLIENT -q "SYSTEM SYNC REPLICA alter_table1"

engine=$($CLICKHOUSE_CLIENT -q "SELECT engine FROM system.tables WHERE database=currentDatabase() AND table='alter_table0'")
if [[ "$engine" == "ReplicatedMergeTree" ]]; then
    # ReplicatedMergeTree may duplicate data on ATTACH PARTITION (when one replica has a merged part and another replica has source parts only)
    $CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE alter_table0 FINAL DEDUPLICATE"
    $CLICKHOUSE_CLIENT -q "SYSTEM SYNC REPLICA alter_table1"
fi

$CLICKHOUSE_CLIENT -q "SELECT count(), sum(b) FROM alter_table0"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(b) FROM alter_table1"

$CLICKHOUSE_CLIENT -q "DROP TABLE alter_table0"
$CLICKHOUSE_CLIENT -q "DROP TABLE alter_table1"
