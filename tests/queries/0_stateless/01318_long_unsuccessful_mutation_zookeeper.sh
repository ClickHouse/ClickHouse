#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS mutation_table"

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE mutation_table(
        key UInt64,
        value String
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_01318/mutation_table', '1')
    ORDER BY key
    PARTITION BY key % 10
"

$CLICKHOUSE_CLIENT --query "INSERT INTO mutation_table select number, toString(number) from numbers(100000) where number % 10 != 0"

$CLICKHOUSE_CLIENT --query "INSERT INTO mutation_table VALUES(0, 'hello')"

$CLICKHOUSE_CLIENT --query "SELECT COUNT() FROM mutation_table"

$CLICKHOUSE_CLIENT --query "ALTER TABLE mutation_table MODIFY COLUMN value UInt64 SETTINGS replication_alter_partitions_sync=0"

first_mutation_id=$($CLICKHOUSE_CLIENT --query "SELECT mutation_id FROM system.mutations where table='mutation_table' and database='$CLICKHOUSE_DATABASE'")

# Here we have long sleeps, but they shouldn't lead to flaps. We just check that
# background mutation finalization function will be triggered at least once. In
# rare cases this test doesn't check anything, but will report OK.
sleep 7

$CLICKHOUSE_CLIENT --query "ALTER TABLE mutation_table MODIFY COLUMN value UInt32 SETTINGS replication_alter_partitions_sync=0"


#### just check that both mutations started
check_query="SELECT count() FROM system.mutations WHERE table='mutation_table' and database='$CLICKHOUSE_DATABASE'"

query_result=$($CLICKHOUSE_CLIENT --query="$check_query" 2>&1)

while [ "$query_result" != "2" ]
do
    query_result=$($CLICKHOUSE_CLIENT --query="$check_query" 2>&1)
    sleep 0.5
done

echo "$query_result"

$CLICKHOUSE_CLIENT --query "KILL MUTATION WHERE mutation_id='$first_mutation_id'"

check_query="SELECT sum(parts_to_do) FROM system.mutations WHERE table='mutation_table' and database='$CLICKHOUSE_DATABASE'"

query_result=$($CLICKHOUSE_CLIENT --query="$check_query" 2>&1)
counter=0

while [ "$query_result" != "1" ]
do
    if [ "$counter" -gt 120 ]
    then
        break
    fi
    query_result=$($CLICKHOUSE_CLIENT --query="$check_query" 2>&1)
    sleep 0.5
    counter=$(($counter + 1))
done


$CLICKHOUSE_CLIENT --query "SELECT is_done, parts_to_do FROM system.mutations where table='mutation_table' and database='$CLICKHOUSE_DATABASE' FORMAT TSVWithNames"

$CLICKHOUSE_CLIENT --query "SELECT type, new_part_name FROM system.replication_queue WHERE table='mutation_table' and database='$CLICKHOUSE_DATABASE'"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS mutation_table"
