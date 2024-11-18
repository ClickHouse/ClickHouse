#!/usr/bin/env bash
# Tags: replica, no-debug

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

SHARD=$($CLICKHOUSE_CLIENT --query "Select getMacro('shard')")
REPLICA=$($CLICKHOUSE_CLIENT --query "Select getMacro('replica')")

# Check that if we have one inactive replica and a huge number of INSERTs to active replicas,
# the number of nodes in ZooKeeper does not grow unbounded.

SCALE=1000

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS r1;
    DROP TABLE IF EXISTS r2;
    CREATE TABLE r1 (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/{shard}', '1{replica}') ORDER BY x
    SETTINGS old_parts_lifetime = 1, cleanup_delay_period = 0, cleanup_delay_period_random_add = 1, cleanup_thread_preferred_points_per_iteration=0, parts_to_throw_insert = 100000, max_replicated_logs_to_keep = 10;
    CREATE TABLE r2 (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/{shard}', '2{replica}') ORDER BY x
    SETTINGS old_parts_lifetime = 1, cleanup_delay_period = 0, cleanup_delay_period_random_add = 1, cleanup_thread_preferred_points_per_iteration=0, parts_to_throw_insert = 100000, max_replicated_logs_to_keep = 10;
    DETACH TABLE r2;
"

# insert_keeper_fault_injection_probability=0 -- can slowdown insert a lot (produce a lot of parts)
$CLICKHOUSE_CLIENT --insert_keeper_fault_injection_probability=0 --max_block_size 1 --min_insert_block_size_rows 1 --min_insert_block_size_bytes 1 --max_insert_threads 16 --query "INSERT INTO r1 SELECT * FROM numbers_mt(${SCALE})"


# Now wait for cleanup thread
for _ in {1..60}; do
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS"
    [[ $($CLICKHOUSE_CLIENT --query "SELECT sum(toUInt32(extract(message, 'Removed (\d+) old log entries'))) FROM system.text_log WHERE event_date >= yesterday() AND logger_name LIKE '%' || '$CLICKHOUSE_DATABASE' || '%r1%(ReplicatedMergeTreeCleanupThread)%' AND message LIKE '%Removed % old log entries%' SETTINGS max_rows_to_read = 0") -gt $((SCALE - 10)) ]] && break;
    sleep 1
done


$CLICKHOUSE_CLIENT --query "SELECT numChildren < $((SCALE / 4)) FROM system.zookeeper WHERE path = '/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/$SHARD' AND name = 'log'";
echo -e '\n---\n';
$CLICKHOUSE_CLIENT --query "SELECT value FROM system.zookeeper WHERE path = '/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/$SHARD/replicas/1$REPLICA' AND name = 'is_lost'";
$CLICKHOUSE_CLIENT --query "SELECT value FROM system.zookeeper WHERE path = '/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/$SHARD/replicas/2$REPLICA' AND name = 'is_lost'";
echo -e '\n---\n';

$CLICKHOUSE_CLIENT --query "ATTACH TABLE r2"
$CLICKHOUSE_CLIENT --receive_timeout 600 --query "SYSTEM SYNC REPLICA r2" # Need to increase timeout, otherwise it timed out in debug build

$CLICKHOUSE_CLIENT --query "SELECT value FROM system.zookeeper WHERE path = '/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/$SHARD/replicas/2$REPLICA' AND name = 'is_lost'";

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS r1;
    DROP TABLE IF EXISTS r2;
"
