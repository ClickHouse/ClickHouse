#!/usr/bin/env bash
# Tags: race, zookeeper, long

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./replication.lib
. "$CURDIR"/replication.lib

export MAX_RETRIES=25

function thread_detach()
{
    while true; do
        # Randomly choose one of the two tables each time.
        { $CLICKHOUSE_CLIENT -m 2>/dev/null <<SQL
ALTER TABLE alter_table$((RANDOM % 2)) DETACH PARTITION ID 'all' SETTINGS log_comment = 'threaded detach';
SELECT sleep($RANDOM / 32000) format Null;
SQL
        } || :
    done
}
function thread_attach()
{
    while true; do
        # Randomly choose one of the two tables each time.
        { $CLICKHOUSE_CLIENT -m 2>/dev/null <<SQL
ALTER TABLE alter_table$((RANDOM % 2)) ATTACH PARTITION ID 'all' SETTINGS log_comment = 'threaded attach';
SELECT sleep($RANDOM / 32000) format Null;
SQL
        } || :
    done
}
function show_cluster_info()
{
    $CLICKHOUSE_CLIENT -q "select cluster, shard_num, replica_num, host_name, port, is_active from system.clusters"
}
function insert()
{
    # Fault injection may lead to duplicates
    if [[ "$insert_type" -eq 0 ]]; then
        $CLICKHOUSE_CLIENT --insert_keeper_fault_injection_probability=0 -q "INSERT INTO alter_table$((RANDOM % 2)) SELECT $RANDOM, $1" 2>/dev/null
    elif [[ "$insert_type" -eq 1 ]]; then
        $CLICKHOUSE_CLIENT -q "INSERT INTO alter_table$((RANDOM % 2)) SELECT $1, $1" 2>/dev/null
    else
        # It may reproduce something interesting: if the insert status is unknown (due to fault injection in retries)
        # and the part was committed locally but not in zk, then it will be active and DETACH may detach it.
        # And we will ATTACH it later. But the next INSERT attempt will not be deduplicated because the first one failed.
        # So we will get duplicates.
        $CLICKHOUSE_CLIENT --insert_deduplication_token="$1" -q "INSERT INTO alter_table$((RANDOM % 2)) SELECT $RANDOM, $1" 2>/dev/null
    fi
}
function do_inserts()
{
    for i in {1..30}; do
        while ! insert "$i"; do $CLICKHOUSE_CLIENT -q "SELECT '$CLICKHOUSE_DATABASE', 'retrying insert $i' FORMAT Null"; done
    done
}

show_cluster_info
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS alter_table0;
    DROP TABLE IF EXISTS alter_table1;

    CREATE TABLE alter_table0 (a UInt8, b Int16) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/alter_table', 'r1') ORDER BY a;
    CREATE TABLE alter_table1 (a UInt8, b Int16) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/alter_table', 'r2') ORDER BY a;
" || exit 1

insert_type=$((RANDOM % 3))

engine=$($CLICKHOUSE_CLIENT -q "SELECT engine FROM system.tables WHERE database=currentDatabase() AND table='alter_table0'")
if [[ "$engine" == "ReplicatedMergeTree" || "$engine" == "SharedMergeTree" ]]; then
    insert_type=$((RANDOM % 2))
fi
$CLICKHOUSE_CLIENT -q "SELECT '$CLICKHOUSE_DATABASE', 'insert_type $insert_type' FORMAT Null"

# Launch detach/attach threads

show_cluster_info
for i in 0 1; do
    thread_detach &
    thread_attach &
done

$CLICKHOUSE_CLIENT -q "SELECT '$CLICKHOUSE_DATABASE', 'begin inserts'"
do_inserts 2>&1| grep -Fa "Exception: " | grep -Fv "was cancelled by concurrent ALTER PARTITION"
$CLICKHOUSE_CLIENT -q "SELECT '$CLICKHOUSE_DATABASE', 'end inserts'"

# Kill detach/attach threads at once
jobs -p | xargs --no-run-if-empty --max-args=10 --max-procs=1 kill -TERM
wait

$CLICKHOUSE_CLIENT -q "SELECT '$CLICKHOUSE_DATABASE', 'threads finished'"
wait_for_queries_to_finish 600
show_cluster_info

$CLICKHOUSE_CLIENT -q "SYSTEM SYNC REPLICA alter_table0"
$CLICKHOUSE_CLIENT -q "SYSTEM SYNC REPLICA alter_table1"
show_cluster_info
query_with_retry "ALTER TABLE alter_table0 ATTACH PARTITION ID 'all'" "ATOMIC_RENAME_FAIL"
query_with_retry "ALTER TABLE alter_table1 ATTACH PARTITION ID 'all'" "ATOMIC_RENAME_FAIL"
$CLICKHOUSE_CLIENT -q "SYSTEM SYNC REPLICA alter_table1"
$CLICKHOUSE_CLIENT -q "ALTER TABLE alter_table1 ATTACH PARTITION ID 'all'"
$CLICKHOUSE_CLIENT -q "SYSTEM SYNC REPLICA alter_table0"
$CLICKHOUSE_CLIENT -q "SYSTEM SYNC REPLICA alter_table1"

if [[ "$engine" == "ReplicatedMergeTree" || "$engine" == "SharedMergeTree" ]]; then
    # ReplicatedMergeTree may duplicate data on ATTACH PARTITION (when one replica has a merged part and another replica has source parts only)
    $CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE alter_table0 FINAL DEDUPLICATE"
    $CLICKHOUSE_CLIENT -q "SYSTEM SYNC REPLICA alter_table1"
fi

$CLICKHOUSE_CLIENT -q "SELECT count(), sum(b) FROM alter_table0"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(b) FROM alter_table1"

$CLICKHOUSE_CLIENT -q "DROP TABLE alter_table0"
$CLICKHOUSE_CLIENT -q "DROP TABLE alter_table1"
