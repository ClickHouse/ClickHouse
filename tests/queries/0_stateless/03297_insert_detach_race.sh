#!/usr/bin/env bash
# Tags: race, zookeeper

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./replication.lib
. "$CURDIR"/replication.lib


$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS insert_detach_race;

    CREATE TABLE insert_detach_race (a UInt8, b Int16)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/insert_detach_race', 'r1')
    ORDER BY a
    SETTINGS sleep_before_commit_local_part_in_replicated_table_ms=5000;
" || exit 1

function thread_detach()
{
    $CLICKHOUSE_CLIENT -q "ALTER TABLE insert_detach_race DETACH PARTITION ID 'all'"
}

$CLICKHOUSE_CLIENT --insert_keeper_fault_injection_probability=0 -q "INSERT INTO insert_detach_race SELECT 1, 2"
$CLICKHOUSE_CLIENT --insert_keeper_fault_injection_probability=0 -q "INSERT INTO insert_detach_race SELECT 3, 4" &

sleep 2
$CLICKHOUSE_CLIENT -q "ALTER TABLE insert_detach_race DETACH PARTITION ID 'all'"

wait
