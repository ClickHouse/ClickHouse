#!/usr/bin/env bash
# Tags: replica, no-shared-merge-tree
# no-shared-merge-tree -- non minimalistic header is not supported

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

SHARD=$($CLICKHOUSE_CLIENT --query "Select getMacro('shard')")
REPLICA=$($CLICKHOUSE_CLIENT --query "Select getMacro('replica')")

$CLICKHOUSE_CLIENT -m -q "


DROP TABLE IF EXISTS part_header_r1;
DROP TABLE IF EXISTS part_header_r2;

SET insert_keeper_fault_injection_probability=0; -- disable fault injection; part ids are non-deterministic in case of insert retries
SET replication_alter_partitions_sync = 2;
-- May affect part names
set prefer_warmed_unmerged_parts_seconds=0;
set ignore_cold_parts_seconds=0;

CREATE TABLE part_header_r1(x UInt32, y UInt32)
    ENGINE ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/test_00814/part_header/{shard}', '1{replica}') ORDER BY x
    SETTINGS use_minimalistic_part_header_in_zookeeper = 0,
             old_parts_lifetime = 1,
             cleanup_delay_period = 0,
             cleanup_delay_period_random_add = 0,
             cleanup_thread_preferred_points_per_iteration=0,
             cache_populated_by_fetch=0;
CREATE TABLE part_header_r2(x UInt32, y UInt32)
    ENGINE ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/test_00814/part_header/{shard}', '2{replica}') ORDER BY x
    SETTINGS use_minimalistic_part_header_in_zookeeper = 1,
             old_parts_lifetime = 1,
             cleanup_delay_period = 0,
             cleanup_delay_period_random_add = 0,
             cleanup_thread_preferred_points_per_iteration=0,
             cache_populated_by_fetch=0;

SELECT '*** Test fetches ***';
INSERT INTO part_header_r1 VALUES (1, 1);
INSERT INTO part_header_r2 VALUES (2, 2);
SYSTEM SYNC REPLICA part_header_r1;
SYSTEM SYNC REPLICA part_header_r2;
SELECT '*** replica 1 ***';
SELECT x, y FROM part_header_r1 ORDER BY x;
SELECT '*** replica 2 ***';
SELECT x, y FROM part_header_r2 ORDER BY x;

SELECT '*** Test merges ***';
OPTIMIZE TABLE part_header_r1;
SYSTEM SYNC REPLICA part_header_r2;
SELECT '*** replica 1 ***';
SELECT _part, x FROM part_header_r1 ORDER BY x;
SELECT '*** replica 2 ***';
SELECT _part, x FROM part_header_r2 ORDER BY x;

"

elapsed=1
until [ $elapsed -eq 5 ];
do
    sleep $(( elapsed++ ))
    count1=$($CLICKHOUSE_CLIENT --query="SELECT count(name) FROM system.zookeeper WHERE path = '/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/test_00814/part_header/$SHARD/replicas/1$REPLICA/parts'")
    count2=$($CLICKHOUSE_CLIENT --query="SELECT count(name) FROM system.zookeeper WHERE path = '/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/test_00814/part_header/$SHARD/replicas/2$REPLICA/parts'")
    [[ $count1 == 1 && $count2 == 1 ]] && break
done

$CLICKHOUSE_CLIENT -m -q "

-- May affect part names
set prefer_warmed_unmerged_parts_seconds=0;
set ignore_cold_parts_seconds=0;

SELECT '*** Test part removal ***';
SELECT '*** replica 1 ***';
SELECT name FROM system.parts WHERE active AND database = currentDatabase() AND table = 'part_header_r1';
SELECT name FROM system.zookeeper WHERE path = '/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/test_00814/part_header/$SHARD/replicas/1$REPLICA/parts';
SELECT '*** replica 2 ***';
SELECT name FROM system.parts WHERE active AND database = currentDatabase() AND table = 'part_header_r2';
SELECT name FROM system.zookeeper WHERE path = '/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/test_00814/part_header/$SHARD/replicas/2$REPLICA/parts';

SELECT '*** Test ALTER ***';
ALTER TABLE part_header_r1 MODIFY COLUMN y String;
SELECT '*** replica 1 ***';
SELECT x, length(y) FROM part_header_r1 ORDER BY x;
SELECT '*** replica 2 ***';
SELECT x, length(y) FROM part_header_r2 ORDER BY x;

SELECT '*** Test CLEAR COLUMN ***';
SET replication_alter_partitions_sync = 2;
ALTER TABLE part_header_r1 CLEAR COLUMN y IN PARTITION tuple();
SELECT '*** replica 1 ***';
SELECT x, length(y) FROM part_header_r1 ORDER BY x;
SELECT '*** replica 2 ***';
SELECT x, length(y) FROM part_header_r2 ORDER BY x;

DROP TABLE part_header_r1;
DROP TABLE part_header_r2;

"
