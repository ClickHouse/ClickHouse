#!/usr/bin/env bash
# Tags: no-fasttest

# https://github.com/ClickHouse/ClickHouse/issues/45328
# Check that replacing one partition on a table with `ALTER TABLE REPLACE PARTITION`
# doesn't wait for mutations on other partitions.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t1;"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t2;"
$CLICKHOUSE_CLIENT -q "
CREATE TABLE t1
(
    p UInt8,
    i UInt64
)
ENGINE = MergeTree
PARTITION BY p
ORDER BY tuple();
"


$CLICKHOUSE_CLIENT -q "INSERT INTO t1 VALUES  (1, 1), (1, 2), (1, 3), (1, 4), (2, 5);"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t2 AS t1;"
$CLICKHOUSE_CLIENT -q "INSERT INTO t2 VALUES (2, 2000);"

# mutation that is supposed to be running in background while REPLACE is performed.
# sleepEachRow(3) is causing a mutation on partition 1 to be stuck. We test that another mutation on an unrelated partition will not wait for this one.
$CLICKHOUSE_CLIENT -q "ALTER TABLE t1 UPDATE i = sleepEachRow(3) IN PARTITION id '1' WHERE p == 1;"

# wait for mutation to start
while [ "$($CLICKHOUSE_CLIENT -q "SELECT is_done as is_running FROM system.mutations WHERE database==currentDatabase() AND table=='t1'")" != 0 ]
do
   sleep .5
done

# Run mutation on another partition
$CLICKHOUSE_CLIENT -q "ALTER TABLE t1 REPLACE PARTITION id '2' FROM t2 SETTINGS mutations_sync=2;"

# check that mutation is still running
$CLICKHOUSE_CLIENT -q "SELECT is_done FROM system.mutations WHERE database==currentDatabase() AND table=='t1';"

$CLICKHOUSE_CLIENT -q "SELECT * FROM t1 ORDER BY i;"

$CLICKHOUSE_CLIENT -q "DROP TABLE t1"
$CLICKHOUSE_CLIENT -q "DROP TABLE t2"
