#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS table_for_bad_alters";

$CLICKHOUSE_CLIENT -n --query "CREATE TABLE table_for_bad_alters (
    key UInt64,
    value1 UInt8,
    value2 String
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/table_for_bad_alters', '1')
ORDER BY key;"

$CLICKHOUSE_CLIENT --query "INSERT INTO table_for_bad_alters VALUES(1, 1, 'Hello');"
$CLICKHOUSE_CLIENT --query "ALTER TABLE table_for_bad_alters MODIFY COLUMN value1 UInt32, DROP COLUMN non_existing_column" 2>&1 | grep -o "Wrong column name." | uniq
$CLICKHOUSE_CLIENT --query "SHOW CREATE TABLE table_for_bad_alters;" # nothing changed

$CLICKHOUSE_CLIENT --query "ALTER TABLE table_for_bad_alters MODIFY COLUMN value2 UInt32 SETTINGS replication_alter_partitions_sync=0;"

sleep 2

while [[ $($CLICKHOUSE_CLIENT --query "KILL MUTATION WHERE mutation_id='0000000000' and database = '$CLICKHOUSE_DATABASE'" 2>&1) ]]; do
    sleep 1
done

$CLICKHOUSE_CLIENT --query "SHOW CREATE TABLE table_for_bad_alters;" # Type changed, but we can revert back

$CLICKHOUSE_CLIENT --query "INSERT INTO table_for_bad_alters VALUES(2, 2, 7)"

$CLICKHOUSE_CLIENT --query "SELECT distinct(value2) FROM table_for_bad_alters" 2>&1 | grep -o 'syntax error at begin of string.' | uniq

$CLICKHOUSE_CLIENT --query "ALTER TABLE table_for_bad_alters MODIFY COLUMN value2 String SETTINGS replication_alter_partitions_sync=2"

$CLICKHOUSE_CLIENT --query "INSERT INTO table_for_bad_alters VALUES(3, 3, 'World')"

$CLICKHOUSE_CLIENT --query "SELECT value2 FROM table_for_bad_alters ORDER BY value2"

$CLICKHOUSE_CLIENT --query "ALTER TABLE table_for_bad_alters DROP INDEX idx2" 2>&1 | grep -o 'Wrong index name.' | uniq

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS table_for_bad_alters"
