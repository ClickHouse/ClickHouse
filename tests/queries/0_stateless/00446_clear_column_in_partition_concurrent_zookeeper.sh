#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

ch="$CLICKHOUSE_CLIENT --stacktrace -q"

$ch "DROP TABLE IF EXISTS clear_column1"
$ch "DROP TABLE IF EXISTS clear_column2"
$ch "CREATE TABLE clear_column1 (d Date, i Int64, s String) ENGINE = ReplicatedMergeTree('/clickhouse/test_00446/tables/clear_column_concurrent', '1', d, d, 8192)"
$ch "CREATE TABLE clear_column2 (d Date, i Int64, s String) ENGINE = ReplicatedMergeTree('/clickhouse/test_00446/tables/clear_column_concurrent', '2', d, d, 8192)"

$ch "ALTER TABLE clear_column1 CLEAR COLUMN VasyaUnexistingColumn IN PARTITION '200001'" --replication_alter_partitions_sync=2 1>/dev/null 2>/dev/null
rc=$?
if [ $rc -eq 0 ]; then
    echo "An unexisisting column was ALTERed. Code: $rc"
    exit 1
fi

set -e
$ch "INSERT INTO clear_column1 VALUES ('2000-01-01', 1, 'a'), ('2000-02-01', 2, 'b')"
$ch "INSERT INTO clear_column1 VALUES ('2000-01-01', 3, 'c'), ('2000-02-01', 4, 'd')"

for _ in $(seq 10); do
    $ch "INSERT INTO clear_column1 VALUES ('2000-02-01', 0, ''), ('2000-02-01', 0, '')" & # insert into the same partition
    $ch "ALTER TABLE clear_column1 CLEAR COLUMN i IN PARTITION '200001'" --replication_alter_partitions_sync=2
    $ch "ALTER TABLE clear_column1 CLEAR COLUMN s IN PARTITION '200001'" --replication_alter_partitions_sync=2
    $ch "ALTER TABLE clear_column1 CLEAR COLUMN i IN PARTITION '200002'" --replication_alter_partitions_sync=2
    $ch "ALTER TABLE clear_column1 CLEAR COLUMN s IN PARTITION '200002'" --replication_alter_partitions_sync=2
    $ch "INSERT INTO clear_column1 VALUES ('2000-03-01', 3, 'c'), ('2000-03-01', 4, 'd')" & # insert into other partition
done
wait

$ch "SELECT DISTINCT * FROM clear_column1 WHERE d != toDate('2000-03-01') ORDER BY d, i, s"
$ch "SELECT DISTINCT * FROM clear_column2 WHERE d != toDate('2000-03-01') ORDER BY d, i, s"

$ch "DROP TABLE clear_column1"
$ch "DROP TABLE clear_column2"
