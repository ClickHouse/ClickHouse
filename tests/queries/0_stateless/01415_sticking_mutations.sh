#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS sticking_mutations"

$CLICKHOUSE_CLIENT -n --query "CREATE TABLE sticking_mutations (
  key UInt64,
  value1 String,
  value2 UInt8
)
ENGINE = MergeTree()
ORDER BY key;"

$CLICKHOUSE_CLIENT --query "INSERT INTO sticking_mutations SELECT number, toString(number), number % 128 FROM numbers(1000)"

# if merges stopped for normal merge tree mutations will stick
$CLICKHOUSE_CLIENT --query "SYSTEM STOP MERGES sticking_mutations"

$CLICKHOUSE_CLIENT --query "ALTER TABLE sticking_mutations DELETE WHERE value2 % 32 == 0, MODIFY COLUMN value1 UInt64;" &


##### wait mutation to start #####
check_query="SELECT count() FROM system.mutations WHERE table='sticking_mutations' and database='$CLICKHOUSE_DATABASE'"

query_result=`$CLICKHOUSE_CLIENT --query="$check_query" 2>&1`

while [ "$query_result" == "0" ]
do
    query_result=`$CLICKHOUSE_CLIENT --query="$check_query" 2>&1`
    sleep 0.5
done
##### wait mutation to start #####

# Starting merges to execute sticked mutations

$CLICKHOUSE_CLIENT --query "SYSTEM START MERGES sticking_mutations"

# just to be sure, that previous mutations finished
$CLICKHOUSE_CLIENT --query "ALTER TABLE sticking_mutations DELETE WHERE value % 31 == 0 SETTINGS mutations_sync = 1"

$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE sticking_mutations FINAL"

$CLICKHOUSE_CLIENT --query "SELECT 1"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS sticking_mutations"
