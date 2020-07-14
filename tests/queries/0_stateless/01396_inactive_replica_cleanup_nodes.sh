#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

# Check that if we have one inactive replica and a huge number of INSERTs to active replicas, 
# the number of nodes in ZooKeeper does not grow unbounded.

$CLICKHOUSE_CLIENT -n --query "
    DROP TABLE IF EXISTS r1;
    DROP TABLE IF EXISTS r2;
    CREATE TABLE r1 (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/r', '1') ORDER BY x SETTINGS old_parts_lifetime = 1, cleanup_delay_period = 1, cleanup_delay_period_random_add = 1, parts_to_throw_insert = 100000, max_replicated_logs_to_keep = 10;
    CREATE TABLE r2 (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/r', '2') ORDER BY x SETTINGS old_parts_lifetime = 1, cleanup_delay_period = 1, cleanup_delay_period_random_add = 1, parts_to_throw_insert = 100000, max_replicated_logs_to_keep = 10;
    DETACH TABLE r2;
"

$CLICKHOUSE_CLIENT --max_block_size 1 --min_insert_block_size_rows 1 --min_insert_block_size_bytes 1 --max_insert_threads 16 --query "INSERT INTO r1 SELECT * FROM numbers_mt(10000)"

$CLICKHOUSE_CLIENT --query "SELECT name, numChildren < 1000 FROM system.zookeeper WHERE path = '/clickhouse/tables/r'";
echo -e '\n---\n';
$CLICKHOUSE_CLIENT --query "SELECT name, numChildren < 1000, name = 'is_lost' ? value : '' FROM system.zookeeper WHERE path = '/clickhouse/tables/r/replicas/1'";
echo -e '\n---\n';
$CLICKHOUSE_CLIENT --query "SELECT name, numChildren < 1000, name = 'is_lost' ? value : '' FROM system.zookeeper WHERE path = '/clickhouse/tables/r/replicas/2'";
echo -e '\n---\n';

$CLICKHOUSE_CLIENT --query "ATTACH TABLE r2"
$CLICKHOUSE_CLIENT --query "SYSTEM SYNC REPLICA r2"

$CLICKHOUSE_CLIENT --query "SELECT name, numChildren < 1000, name = 'is_lost' ? value : '' FROM system.zookeeper WHERE path = '/clickhouse/tables/r/replicas/2'";

$CLICKHOUSE_CLIENT -n --query "
    DROP TABLE IF EXISTS r1;
    DROP TABLE IF EXISTS r2;
"
