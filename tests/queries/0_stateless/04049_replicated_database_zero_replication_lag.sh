#!/usr/bin/env bash

# Tags: zookeeper, no-fasttest

# Regression test: when max_replication_lag_to_enqueue=0, the replica should not
# be falsely marked as unsynced, and the chassert in entry processing should not
# fire when our_log_ptr catches up to max_log_ptr.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

db="rdb_zero_lag_$CLICKHOUSE_DATABASE"

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $db"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE $db ENGINE=Replicated('/test/$CLICKHOUSE_DATABASE/rdb_zero_lag', 's1', 'r1') SETTINGS max_replication_lag_to_enqueue=0"

$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none -q "CREATE TABLE $db.t1 (n Int32) ENGINE=MergeTree ORDER BY n"
$CLICKHOUSE_CLIENT -q "INSERT INTO $db.t1 VALUES (1), (2), (3)"

$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none -q "CREATE TABLE $db.t2 (s String) ENGINE=MergeTree ORDER BY s"
$CLICKHOUSE_CLIENT -q "INSERT INTO $db.t2 VALUES ('a'), ('b')"

# Verify tables are accessible and data is correct
$CLICKHOUSE_CLIENT -q "SELECT count() FROM $db.t1"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM $db.t2"

# Verify the replica is not read-only (which would happen if falsely marked as lagging)
$CLICKHOUSE_CLIENT -q "SELECT is_readonly FROM system.database_replicas WHERE database = '$db'"

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $db"
