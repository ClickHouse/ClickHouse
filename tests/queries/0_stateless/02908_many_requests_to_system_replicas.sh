#!/usr/bin/env bash
# Tags: long, zookeeper, no-parallel, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

NUM_TABLES=300
CONCURRENCY=200

echo "Creating $NUM_TABLES tables"

function get_done_or_die_trying()
{
  # Sometimes curl produces errors like 'Recv failure: Connection reset by peer' and fails test, let's add a little bit of retries
  for _ in $(seq 1 10)
  do
    curl "$CLICKHOUSE_URL" --silent --fail --show-error --data "$1" &>/dev/null && return
  done

  echo "Cannot successfully make request"
  exit 1
}

function init_table()
{
    set -e
    i=$1
    get_done_or_die_trying "CREATE TABLE test_02908_r1_$i (a UInt64) ENGINE=ReplicatedMergeTree('/02908/{database}/test_$i', 'r1') ORDER BY tuple()"
    get_done_or_die_trying "CREATE TABLE test_02908_r2_$i (a UInt64) ENGINE=ReplicatedMergeTree('/02908/{database}/test_$i', 'r2') ORDER BY tuple()"
    get_done_or_die_trying "CREATE TABLE test_02908_r3_$i (a UInt64) ENGINE=ReplicatedMergeTree('/02908/{database}/test_$i', 'r3') ORDER BY tuple()"

    get_done_or_die_trying "INSERT INTO test_02908_r1_$i  SELECT rand64() FROM numbers(5);"
}

export init_table;

for i in $(seq 1 $NUM_TABLES)
do
    init_table "$i" &
done

wait;


# Check results with different max_block_size
$CLICKHOUSE_CLIENT -q 'SELECT count(), sum(total_replicas) >= 2700, sum(active_replicas) >= 2700 FROM system.replicas WHERE database=currentDatabase()'
$CLICKHOUSE_CLIENT -q 'SELECT count(), sum(total_replicas) >= 2700, sum(active_replicas) >= 2700 FROM system.replicas WHERE database=currentDatabase() SETTINGS max_block_size=1'
$CLICKHOUSE_CLIENT -q 'SELECT count(), sum(total_replicas) >= 2700, sum(active_replicas) >= 2700 FROM system.replicas WHERE database=currentDatabase() SETTINGS max_block_size=77'
$CLICKHOUSE_CLIENT -q 'SELECT count(), sum(total_replicas) >= 2700, sum(active_replicas) >= 2700 FROM system.replicas WHERE database=currentDatabase() SETTINGS max_block_size=11111'


echo "Making $CONCURRENCY requests to system.replicas"

for i in $(seq 1 $CONCURRENCY)
do
    curl "$CLICKHOUSE_URL" --silent --fail --show-error --data "SELECT * FROM system.replicas WHERE database=currentDatabase() FORMAT Null SETTINGS log_comment='02908_many_requests';" &>/dev/null &
done

echo "Query system.replicas while waiting for other concurrent requests to finish"
# lost_part_count column is read from ZooKeeper
curl "$CLICKHOUSE_URL" --silent --fail --show-error --data "SELECT sum(lost_part_count) FROM system.replicas WHERE database=currentDatabase();" 2>&1;
# is_leader column is filled without ZooKeeper
curl "$CLICKHOUSE_URL" --silent --fail --show-error --data "SELECT sum(is_leader) FROM system.replicas WHERE database=currentDatabase();" 2>&1;

wait;

$CLICKHOUSE_CLIENT -nq "
SYSTEM FLUSH LOGS;

-- without optimisation there are ~350K zk requests
SELECT sum(ProfileEvents['ZooKeeperTransactions']) < 30000
  FROM system.query_log
 WHERE current_database=currentDatabase() AND log_comment='02908_many_requests';
"
