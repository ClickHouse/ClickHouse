#!/usr/bin/env bash
# Tags: no-replicated-database

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS sticking_mutations"

function check_sticky_mutations()
{
    $CLICKHOUSE_CLIENT --query "CREATE TABLE sticking_mutations (
      date Date,
      key UInt64,
      value1 String,
      value2 UInt8
    )
    ENGINE = MergeTree()
    ORDER BY key;"

    $CLICKHOUSE_CLIENT --query "INSERT INTO sticking_mutations SELECT toDate('2020-07-10'), number, toString(number), number % 128 FROM numbers(1000)"

    $CLICKHOUSE_CLIENT --query "INSERT INTO sticking_mutations SELECT toDate('2100-01-10'), number, toString(number), number % 128 FROM numbers(1000)"

    # if merges stopped for normal merge tree mutations will stick
    $CLICKHOUSE_CLIENT --query "SYSTEM STOP MERGES sticking_mutations"

    $CLICKHOUSE_CLIENT --query "$1" &

    ##### wait mutation to start #####
    check_query="SELECT count() FROM system.mutations WHERE table='sticking_mutations' and database='$CLICKHOUSE_DATABASE' and is_done = 0"

    query_result=$($CLICKHOUSE_CLIENT --query="$check_query" 2>&1)

    for _ in {1..50}
    do
        query_result=$($CLICKHOUSE_CLIENT --query="$check_query" 2>&1)
        if ! [ "$query_result" == "0" ]; then break; fi
        sleep 0.5
    done
    ##### wait mutation to start #####

    # Starting merges to execute sticked mutations

    $CLICKHOUSE_CLIENT --query "SYSTEM START MERGES sticking_mutations"

    # Just to be sure, that previous mutations finished
    $CLICKHOUSE_CLIENT --query "ALTER TABLE sticking_mutations DELETE WHERE value2 % 31 == 0 SETTINGS mutations_sync = 1"

    $CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE sticking_mutations FINAL"

    $CLICKHOUSE_CLIENT --query "SELECT sum(cityHash64(*)) > 1 FROM sticking_mutations WHERE key > 10"

    $CLICKHOUSE_CLIENT --query "SHOW CREATE TABLE sticking_mutations"

    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS sticking_mutations"
}

check_sticky_mutations "ALTER TABLE sticking_mutations DELETE WHERE value2 % 32 == 0, MODIFY COLUMN value1 UInt64"

check_sticky_mutations "ALTER TABLE sticking_mutations MODIFY COLUMN value1 UInt64, DELETE WHERE value2 % 32 == 0"

check_sticky_mutations "ALTER TABLE sticking_mutations UPDATE value1 = 15 WHERE key < 2000, DELETE WHERE value2 % 32 == 0"

check_sticky_mutations "ALTER TABLE sticking_mutations DELETE WHERE value2 % 32 == 0, UPDATE value1 = 15 WHERE key < 2000"

check_sticky_mutations "ALTER TABLE sticking_mutations DELETE WHERE value2 % 32 == 0, DROP COLUMN value1"

check_sticky_mutations "ALTER TABLE sticking_mutations DELETE WHERE value2 % 32 == 0, RENAME COLUMN value1 TO renamed_value1"

check_sticky_mutations "ALTER TABLE sticking_mutations MODIFY COLUMN value1 UInt64, MODIFY TTL date + INTERVAL 1 DAY"
