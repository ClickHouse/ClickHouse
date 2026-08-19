#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-fasttest, no-shared-merge-tree
# Tag no-replicated-database: Requires investigation
# no-shared-merge-tree: relies on zookeeper structure of rmt

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

CLICKHOUSE_TEST_ZOOKEEPER_PREFIX="${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/${CLICKHOUSE_DATABASE}"
SHARD=$($CLICKHOUSE_CLIENT --query "Select getMacro('shard')")

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS elog;"

$CLICKHOUSE_CLIENT --query="
CREATE TABLE elog (
    date Date,
    engine_id UInt32,
    referrer String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/elog/{shard}', '{replica}')
PARTITION BY date
ORDER BY (engine_id)
SETTINGS replicated_deduplication_window = 2, cleanup_delay_period=4, cleanup_delay_period_random_add=0, cleanup_thread_preferred_points_per_iteration=0;"

$CLICKHOUSE_CLIENT --query="INSERT INTO elog VALUES (toDate('2018-10-01'), 1, 'hello')"
$CLICKHOUSE_CLIENT --query="INSERT INTO elog VALUES (toDate('2018-10-01'), 2, 'hello')"
$CLICKHOUSE_CLIENT --query="INSERT INTO elog VALUES (toDate('2018-10-01'), 3, 'hello')"

$CLICKHOUSE_CLIENT --query="SELECT count(*) from elog" # 3 rows

count=$($CLICKHOUSE_CLIENT --query="SELECT COUNT(*) FROM system.zookeeper where path = '/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/elog/$SHARD/blocks'")
while [[ $count != 2 ]]
do
    sleep 1
    count=$($CLICKHOUSE_CLIENT --query="SELECT COUNT(*) FROM system.zookeeper where path = '/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/elog/$SHARD/blocks'")
done

$CLICKHOUSE_CLIENT --query="INSERT INTO elog VALUES (toDate('2018-10-01'), 1, 'hello')"

$CLICKHOUSE_CLIENT --query="SELECT count(*) from elog" # 4 rows

count=$($CLICKHOUSE_CLIENT --query="SELECT COUNT(*) FROM system.zookeeper where path = '/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/elog/$SHARD/blocks'")
while [[ $count != 2 ]]
do
    sleep 1
    count=$($CLICKHOUSE_CLIENT --query="SELECT COUNT(*) FROM system.zookeeper where path = '/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/elog/$SHARD/blocks'")
done

$CLICKHOUSE_CLIENT --query="INSERT INTO elog VALUES (toDate('2018-10-01'), 2, 'hello')"

$CLICKHOUSE_CLIENT --query="SELECT count(*) from elog" # 5 rows

count=$($CLICKHOUSE_CLIENT --query="SELECT COUNT(*) FROM system.zookeeper where path = '/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/elog/$SHARD/blocks'")
while [[ $count != 2 ]]
do
    sleep 1
    count=$($CLICKHOUSE_CLIENT --query="SELECT COUNT(*) FROM system.zookeeper where path = '/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/elog/$SHARD/blocks'")
done

$CLICKHOUSE_CLIENT --query="INSERT INTO elog VALUES (toDate('2018-10-01'), 2, 'hello')"

$CLICKHOUSE_CLIENT --query="SELECT count(*) from elog" # still 5 rows

$CLICKHOUSE_CLIENT -q "DROP TABLE elog"
