#!/usr/bin/env bash
# Tags: zookeeper, replica, no-shared-merge-tree, no-replicated-database
# no-shared-merge-tree: the setting bounds the ReplicatedMergeTree log, which SharedMergeTree does not have
# no-replicated-database: the ZooKeeper path and the replica name are literal, so they collide between the hosts of the database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The oldest kept log record names the threshold that log pointers of inactive replicas are compared
# with, so a window that keeps no records at all has no meaning.
ERR="A setting's value has to be greater than 0"

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE max_logs_zero (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/r', '1') ORDER BY x
    SETTINGS max_replicated_logs_to_keep = 0;
" 2>&1 | grep -F -q "$ERR" && echo 1 || echo 0

# An ATTACH that carries a full definition is user input too, and reaches the same check.
uuid=$($CLICKHOUSE_CLIENT --query "SELECT generateUUIDv4()")
$CLICKHOUSE_CLIENT --query "
    ATTACH TABLE max_logs_zero UUID '$uuid' (x UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/r', '1') ORDER BY x
    SETTINGS max_replicated_logs_to_keep = 0;
" 2>&1 | grep -F -q "$ERR" && echo 1 || echo 0

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE max_logs (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/r2', '1') ORDER BY x;
"
$CLICKHOUSE_CLIENT --query "
    ALTER TABLE max_logs MODIFY SETTING max_replicated_logs_to_keep = 0;
" 2>&1 | grep -F -q "$ERR" && echo 1 || echo 0

# A positive value is still accepted and stored.
$CLICKHOUSE_CLIENT --query "ALTER TABLE max_logs MODIFY SETTING max_replicated_logs_to_keep = 1;"
$CLICKHOUSE_CLIENT --query "
    SELECT count() FROM system.tables
    WHERE database = currentDatabase() AND name = 'max_logs'
      AND engine_full LIKE '%max_replicated_logs_to_keep = 1%';
"

$CLICKHOUSE_CLIENT --query "DROP TABLE max_logs SYNC"
