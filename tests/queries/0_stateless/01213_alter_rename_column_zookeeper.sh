#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS table_for_rename_replicated"

$CLICKHOUSE_CLIENT -n --query "
CREATE TABLE table_for_rename_replicated
(
  date Date,
  key UInt64,
  value1 String,
  value2 String,
  value3 String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/table_for_rename_replicated', '1')
PARTITION BY date
ORDER BY key;
"


$CLICKHOUSE_CLIENT --query "INSERT INTO table_for_rename_replicated SELECT toDate('2019-10-01') + number % 3, number, toString(number), toString(number), toString(number) from numbers(9);"

$CLICKHOUSE_CLIENT --query "SELECT value1 FROM table_for_rename_replicated WHERE key = 1;"

$CLICKHOUSE_CLIENT --query "SYSTEM STOP MERGES table_for_rename_replicated;"

$CLICKHOUSE_CLIENT --query "SHOW CREATE TABLE table_for_rename_replicated;"

$CLICKHOUSE_CLIENT --query "ALTER TABLE table_for_rename_replicated RENAME COLUMN value1 to renamed_value1" --replication_alter_partitions_sync=0


while [[ -z $($CLICKHOUSE_CLIENT --query "SELECT name FROM system.columns WHERE name = 'renamed_value1' and table = 'table_for_rename_replicated'" 2>/dev/null) ]]; do
    sleep 0.5
done

$CLICKHOUSE_CLIENT --query "SELECT name FROM system.columns WHERE name = 'renamed_value1' and table = 'table_for_rename_replicated'"

# SHOW CREATE TABLE takes query from .sql file on disk.
# previous select take metadata from memory. So, when previous select says, that return renamed_value1 already exists in table, it's still can have old version on disk.
while [[ -z $($CLICKHOUSE_CLIENT --query "SHOW CREATE TABLE table_for_rename_replicated;" | grep 'renamed_value1') ]]; do
    sleep 0.5
done

$CLICKHOUSE_CLIENT --query "SHOW CREATE TABLE table_for_rename_replicated;"

$CLICKHOUSE_CLIENT --query "SELECT renamed_value1 FROM table_for_rename_replicated WHERE key = 1;"

$CLICKHOUSE_CLIENT --query "SELECT * FROM table_for_rename_replicated WHERE key = 1 FORMAT TSVWithNames;"

$CLICKHOUSE_CLIENT --query "SYSTEM START MERGES table_for_rename_replicated;"

$CLICKHOUSE_CLIENT --query "SYSTEM SYNC REPLICA table_for_rename_replicated;"

$CLICKHOUSE_CLIENT --query "SELECT * FROM table_for_rename_replicated WHERE key = 1 FORMAT TSVWithNames;"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS table_for_rename_replicated;"
