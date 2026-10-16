#!/usr/bin/env bash
# Tags: zookeeper, no-shared-merge-tree, no-replicated-database, no-ordinary-database
# Tag no-shared-merge-tree: the test hand-crafts the ZooKeeper nodes of a ReplicatedMergeTree replica.
# Tag no-replicated-database: the test creates an explicit second replica of one table.
# Tag no-ordinary-database: the test creates a table with an explicit UUID.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A replica that failed after creating its ZooKeeper nodes but before saving the local metadata is
# recognized and reused when the table is created again. The nodes may have been written by a server
# that kept the redundant parentheses the user wrote (26.5..26.7), so `createReplicaAttempt` must
# compare its `metadata` and `columns` structurally, not as raw strings.

ZK_PATH="/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_parens_recovery"
UUID=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_parens_recovery_r1 SYNC"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_parens_recovery_r1 (x UInt64, y UInt64 DEFAULT x + 1)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_parens_recovery', 'r1') ORDER BY (x)"

# Simulate the leftover nodes of a replica r2 that a 26.5..26.7 server created in ZooKeeper before
# failing to save the local metadata: the same definitions, spelled with the redundant parentheses.
# First prove that the parenthesized spelling actually differs from the stored one.
$CLICKHOUSE_CLIENT -q "
    WITH (SELECT value FROM system.zookeeper WHERE path = '$ZK_PATH' AND name = 'metadata') AS m,
         (SELECT value FROM system.zookeeper WHERE path = '$ZK_PATH' AND name = 'columns') AS c
    SELECT replaceOne(m, 'primary key: x\n', 'primary key: (x)\n') != m,
           replaceOne(c, 'DEFAULT\tx + 1', 'DEFAULT\t(x + 1)') != c"

$CLICKHOUSE_CLIENT -q "
    INSERT INTO system.zookeeper (path, name, value)
    WITH (SELECT value FROM system.zookeeper WHERE path = '$ZK_PATH' AND name = 'metadata') AS m,
         (SELECT value FROM system.zookeeper WHERE path = '$ZK_PATH' AND name = 'columns') AS c
    SELECT '$ZK_PATH/replicas/r2', name, value
    FROM values('name String, value String',
        ('host', ''),
        ('log_pointer', ''),
        ('queue', ''),
        ('parts', ''),
        ('flags', ''),
        ('is_lost', '1'),
        ('metadata_version', '0'),
        ('min_unprocessed_insert_time', ''),
        ('max_processed_insert_time', ''),
        ('mutation_pointer', ''))
    UNION ALL
    SELECT '$ZK_PATH/replicas/r2', 'metadata', replaceOne(m, 'primary key: x\n', 'primary key: (x)\n')
    UNION ALL
    SELECT '$ZK_PATH/replicas/r2', 'columns', replaceOne(c, 'DEFAULT\tx + 1', 'DEFAULT\t(x + 1)')
    UNION ALL
    SELECT '$ZK_PATH/replicas/r2', 'creator_info', concat('$UUID', '|', toString(serverUUID()))"

# Retrying the creation of r2 with the same definitions (written with the redundant parentheses,
# as the failed server would have them) reuses the existing empty replica instead of throwing
# REPLICA_ALREADY_EXISTS.
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_parens_recovery_r2 UUID '$UUID' (x UInt64, y UInt64 DEFAULT (x + 1))
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_parens_recovery', 'r2') ORDER BY ((x))"

$CLICKHOUSE_CLIENT -q "INSERT INTO t_parens_recovery_r1 (x) VALUES (1)"
$CLICKHOUSE_CLIENT -q "SYSTEM SYNC REPLICA t_parens_recovery_r2"
$CLICKHOUSE_CLIENT -q "SELECT x, y FROM t_parens_recovery_r2"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_parens_recovery_r2 SYNC"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_parens_recovery_r1 SYNC"
