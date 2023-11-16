#!/usr/bin/env bash
# Tags: long, zookeeper, no-parallel, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

NUM_TABLES=300
CONCURRENCY=500

echo "Creating $NUM_TABLES tables"

function init_table()
{
    i=$1
    curl $CLICKHOUSE_URL --silent --fail --data "CREATE TABLE test_02908_r1_$i (a UInt64) ENGINE=ReplicatedMergeTree('/02908/{database}/test_$i', 'r1') ORDER BY tuple()"
    curl $CLICKHOUSE_URL --silent --fail --data "CREATE TABLE test_02908_r2_$i (a UInt64) ENGINE=ReplicatedMergeTree('/02908/{database}/test_$i', 'r2') ORDER BY tuple()"
    curl $CLICKHOUSE_URL --silent --fail --data "CREATE TABLE test_02908_r3_$i (a UInt64) ENGINE=ReplicatedMergeTree('/02908/{database}/test_$i', 'r3') ORDER BY tuple()"

    curl $CLICKHOUSE_URL --silent --fail --data "INSERT INTO test_02908_r1_$i  SELECT rand64() FROM numbers(5);"
}

export init_table;

for i in `seq 1 $NUM_TABLES`;
do
    init_table $i &
done

wait;


echo "Making making $CONCURRENCY requests to system.replicas"

for i in `seq 1 $CONCURRENCY`;
do
    curl $CLICKHOUSE_URL --silent --fail --data "SELECT * FROM system.replicas WHERE database=currentDatabase() FORMAT Null;" &
done

echo "Query system.replicas while waiting for other concurrent requests to finish"
# lost_part_count column is read from ZooKeeper
curl $CLICKHOUSE_URL --silent --fail --data "SELECT sum(lost_part_count) FROM system.replicas WHERE database=currentDatabase();";
# is_leader column is filled without ZooKeeper
curl $CLICKHOUSE_URL --silent --fail --data "SELECT sum(is_leader) FROM system.replicas WHERE database=currentDatabase();";

wait;
