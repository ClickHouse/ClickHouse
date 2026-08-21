#!/usr/bin/env bash
# Tags: long, zookeeper, no-parallel, no-fasttest, no-asan

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

NUM_TABLES=50
CONCURRENCY=100

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
$CLICKHOUSE_CLIENT -q 'SELECT count() as c, sum(total_replicas) >= 3*c, sum(active_replicas) >= 3*c FROM system.replicas WHERE database=currentDatabase()'
$CLICKHOUSE_CLIENT -q 'SELECT count() as c, sum(total_replicas) >= 3*c, sum(active_replicas) >= 3*c FROM system.replicas WHERE database=currentDatabase() SETTINGS max_block_size=1'
$CLICKHOUSE_CLIENT -q 'SELECT count() as c, sum(total_replicas) >= 3*c, sum(active_replicas) >= 3*c FROM system.replicas WHERE database=currentDatabase() SETTINGS max_block_size=77'
$CLICKHOUSE_CLIENT -q 'SELECT count() as c, sum(total_replicas) >= 3*c, sum(active_replicas) >= 3*c FROM system.replicas WHERE database=currentDatabase() SETTINGS max_block_size=11111'


echo "Making $CONCURRENCY requests to system.replicas"

for i in $(seq 1 $CONCURRENCY)
do
    curl "$CLICKHOUSE_URL" --silent --fail --show-error --data "
        SELECT * FROM system.replicas WHERE database=currentDatabase() FORMAT Null SETTINGS log_comment='02908_many_requests';" &>/dev/null &
done

echo "Query system.replicas while waiting for other concurrent requests to finish"
# lost_part_count column is read from ZooKeeper
curl "$CLICKHOUSE_URL" --silent --fail --show-error --data "SELECT sum(lost_part_count) FROM system.replicas WHERE database=currentDatabase();" 2>&1;
# is_leader column is filled without ZooKeeper
curl "$CLICKHOUSE_URL" --silent --fail --show-error --data "SELECT sum(is_leader) FROM system.replicas WHERE database=currentDatabase();" 2>&1;

wait;


# Make a request to system.replicas without other concurrent requests to figure out the baseline of how many ZK requests it makes
curl "$CLICKHOUSE_URL" --silent --fail --show-error --data "
   SELECT * FROM system.replicas WHERE database=currentDatabase() FORMAT Null SETTINGS log_comment='02908_many_requests-baseline';" &>/dev/null


# Wait for the baseline query to appear in query_log.
# There is a race between HTTP response being sent and the query_log entry being written.
for _ in $(seq 1 60); do
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
    count=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.query_log WHERE current_database=currentDatabase() AND log_comment='02908_many_requests-baseline' AND type = 'QueryFinish'")
    [ "$count" -ge 1 ] && break
    sleep 0.5
done

$CLICKHOUSE_CLIENT -q "
-- Check that average number of ZK request is less then a half of max requests
WITH
    (SELECT ProfileEvents['ZooKeeperTransactions']
    FROM system.query_log
    WHERE current_database=currentDatabase() AND log_comment='02908_many_requests-baseline' AND type = 'QueryFinish') AS max_zookeeper_requests
SELECT
    if (sum(ProfileEvents['ZooKeeperTransactions']) <= (max_zookeeper_requests * ${CONCURRENCY} / 2) as passed,
        passed::String,
        'More ZK requests then expected: max_zookeeper_requests=' || max_zookeeper_requests::String || '  total_zk_requests=' || sum(ProfileEvents['ZooKeeperTransactions'])::String
    )
FROM system.query_log
WHERE current_database=currentDatabase() AND log_comment='02908_many_requests' AND type = 'QueryFinish';
"
