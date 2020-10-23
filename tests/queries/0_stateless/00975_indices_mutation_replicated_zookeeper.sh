#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh
. $CURDIR/mergetree_mutations.lib

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS test.indices_mutaions1;"
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS test.indices_mutaions2;"


$CLICKHOUSE_CLIENT -n --query="
CREATE TABLE test.indices_mutaions1
(
    u64 UInt64,
    i64 Int64,
    i32 Int32,
    INDEX idx (i64, u64 * i64) TYPE minmax GRANULARITY 1
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/indices_mutaions', 'r1')
PARTITION BY i32
ORDER BY u64
SETTINGS index_granularity = 2;
CREATE TABLE test.indices_mutaions2
(
    u64 UInt64,
    i64 Int64,
    i32 Int32,
    INDEX idx (i64, u64 * i64) TYPE minmax GRANULARITY 1
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/indices_mutaions', 'r2')
PARTITION BY i32
ORDER BY u64
SETTINGS index_granularity = 2;"


$CLICKHOUSE_CLIENT --query="INSERT INTO test.indices_mutaions1 VALUES
(0, 2, 1),
(1, 1, 1),
(2, 1, 1),
(3, 1, 1),
(4, 1, 1),
(5, 2, 1),
(6, 1, 2),
(7, 1, 2),
(8, 1, 2),
(9, 1, 2)"

$CLICKHOUSE_CLIENT --query="SELECT count() FROM test.indices_mutaions2 WHERE i64 = 2;"
$CLICKHOUSE_CLIENT --query="SELECT count() FROM test.indices_mutaions2 WHERE i64 = 2 FORMAT JSON;" | grep "rows_read"

$CLICKHOUSE_CLIENT --query="ALTER TABLE test.indices_mutaions1 CLEAR INDEX idx IN PARTITION 1;"
sleep 1

$CLICKHOUSE_CLIENT --query="SELECT count() FROM test.indices_mutaions2 WHERE i64 = 2;"
$CLICKHOUSE_CLIENT --query="SELECT count() FROM test.indices_mutaions2 WHERE i64 = 2 FORMAT JSON;" | grep "rows_read"

$CLICKHOUSE_CLIENT --query="ALTER TABLE test.indices_mutaions1 MATERIALIZE INDEX idx IN PARTITION 1;"
wait_for_mutation "indices_mutaions1" "0000000000" "test"
wait_for_mutation "indices_mutaions2" "0000000000" "test"

$CLICKHOUSE_CLIENT --query="SELECT count() FROM test.indices_mutaions2 WHERE i64 = 2;"
$CLICKHOUSE_CLIENT --query="SELECT count() FROM test.indices_mutaions2 WHERE i64 = 2 FORMAT JSON;" | grep "rows_read"

$CLICKHOUSE_CLIENT --query="DROP TABLE test.indices_mutaions1"
$CLICKHOUSE_CLIENT --query="DROP TABLE test.indices_mutaions2"
