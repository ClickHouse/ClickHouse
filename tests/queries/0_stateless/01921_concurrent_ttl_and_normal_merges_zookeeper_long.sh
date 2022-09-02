#!/usr/bin/env bash
# Tags: long, zookeeper, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./replication.lib
. "$CURDIR"/replication.lib

NUM_REPLICAS=5

for i in $(seq 1 $NUM_REPLICAS); do
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ttl_table$i" &
done

wait

for i in $(seq 1 $NUM_REPLICAS); do
    $CLICKHOUSE_CLIENT -n --query "CREATE TABLE ttl_table$i(
        key DateTime
    )
    ENGINE ReplicatedMergeTree('/test/01921_concurrent_ttl_and_normal_merges/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/ttl_table', '$i')
    ORDER BY tuple()
    TTL key + INTERVAL 1 SECOND
    SETTINGS merge_with_ttl_timeout=1, max_replicated_merges_with_ttl_in_queue=100, max_number_of_merges_with_ttl_in_pool=100, cleanup_delay_period=1, cleanup_delay_period_random_add=0;"
done

function optimize_thread
{
    while true; do
        REPLICA=$(($RANDOM % 5 + 1))
        $CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE ttl_table$REPLICA FINAl"
    done
}

function insert_thread
{
    while true; do
        REPLICA=$(($RANDOM % 5 + 1))
        $CLICKHOUSE_CLIENT --optimize_on_insert=0 --query "INSERT INTO ttl_table$REPLICA SELECT now() + rand() % 5 - rand() % 3 FROM numbers(5)"
        $CLICKHOUSE_CLIENT --optimize_on_insert=0 --query "INSERT INTO ttl_table$REPLICA SELECT now() + rand() % 5 - rand() % 3 FROM numbers(5)"
        $CLICKHOUSE_CLIENT --optimize_on_insert=0 --query "INSERT INTO ttl_table$REPLICA SELECT now() + rand() % 5 - rand() % 3 FROM numbers(5)"
    done
}


export -f insert_thread;
export -f optimize_thread;

TIMEOUT=30

timeout $TIMEOUT bash -c insert_thread 2> /dev/null &
timeout $TIMEOUT bash -c insert_thread 2> /dev/null &
timeout $TIMEOUT bash -c insert_thread 2> /dev/null &
timeout $TIMEOUT bash -c insert_thread 2> /dev/null &
timeout $TIMEOUT bash -c insert_thread 2> /dev/null &
timeout $TIMEOUT bash -c optimize_thread 2> /dev/null &
timeout $TIMEOUT bash -c optimize_thread 2> /dev/null &
timeout $TIMEOUT bash -c optimize_thread 2> /dev/null &
timeout $TIMEOUT bash -c optimize_thread 2> /dev/null &
timeout $TIMEOUT bash -c optimize_thread 2> /dev/null &

wait
for i in $(seq 1 $NUM_REPLICAS); do
    # disable ttl merges before checking consistency
    $CLICKHOUSE_CLIENT --query "ALTER TABLE ttl_table$i MODIFY SETTING max_replicated_merges_with_ttl_in_queue=0"
done
check_replication_consistency "ttl_table" "count(), sum(toUInt64(key))"

$CLICKHOUSE_CLIENT --query "SELECT * FROM system.replication_queue where table like 'ttl_table%' and database = '${CLICKHOUSE_DATABASE}' and type='MERGE_PARTS' and last_exception != '' FORMAT Vertical"
$CLICKHOUSE_CLIENT --query "SELECT COUNT() > 0 FROM system.part_log where table like 'ttl_table%' and database = '${CLICKHOUSE_DATABASE}'"


for i in $(seq 1 $NUM_REPLICAS); do
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ttl_table$i" &
done
wait
