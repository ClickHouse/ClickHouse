#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

. "$CURDIR"/mergetree_mutations.lib

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.kill_mutation"

${CLICKHOUSE_CLIENT} --query="CREATE TABLE test.kill_mutation(d Date, x UInt32, s String) ENGINE MergeTree ORDER BY x PARTITION BY d"

${CLICKHOUSE_CLIENT} --query="INSERT INTO test.kill_mutation VALUES ('2000-01-01', 1, 'a')"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test.kill_mutation VALUES ('2001-01-01', 2, 'b')"

${CLICKHOUSE_CLIENT} --query="SELECT '*** Create and kill a single invalid mutation ***'"

${CLICKHOUSE_CLIENT} --query="ALTER TABLE test.kill_mutation DELETE WHERE toUInt32(s) = 1 SETTINGS mutations_sync = 1" 2>/dev/null &


check_query1="SELECT count() FROM system.mutations WHERE database = 'test' AND table = 'kill_mutation' AND is_done = 0"

query_result=$($CLICKHOUSE_CLIENT --query="$check_query1" 2>&1)

while [ "$query_result" == "0" ]
do
    query_result=$($CLICKHOUSE_CLIENT --query="$check_query1" 2>&1)
    sleep 0.5
done

${CLICKHOUSE_CLIENT} --query="SELECT count() FROM system.mutations WHERE database = 'test' AND table = 'kill_mutation' and is_done = 0"

kill_message=$(${CLICKHOUSE_CLIENT} --query="KILL MUTATION WHERE database = 'test' AND table = 'kill_mutation'")

wait

echo "$kill_message"

${CLICKHOUSE_CLIENT} --query="SELECT mutation_id FROM system.mutations WHERE database = 'test' AND table = 'kill_mutation'"


${CLICKHOUSE_CLIENT} --query="SELECT '*** Create and kill invalid mutation that blocks another mutation ***'"

${CLICKHOUSE_CLIENT} --query="ALTER TABLE test.kill_mutation DELETE WHERE toUInt32(s) = 1"
${CLICKHOUSE_CLIENT} --query="ALTER TABLE test.kill_mutation DELETE WHERE x = 1 SETTINGS mutations_sync = 1" 2>&1 | grep -o "happened during execution of mutations 'mutation_4.txt, mutation_5.txt'" | head -n 1 &

check_query2="SELECT count() FROM system.mutations WHERE database = 'test' AND table = 'kill_mutation' AND mutation_id = 'mutation_4.txt'"

query_result=$($CLICKHOUSE_CLIENT --query="$check_query2" 2>&1)

while [ "$query_result" == "0" ]
do
    query_result=$($CLICKHOUSE_CLIENT --query="$check_query2" 2>&1)
    sleep 0.5
done

${CLICKHOUSE_CLIENT} --query="SELECT count() FROM system.mutations WHERE database = 'test' AND table = 'kill_mutation' AND mutation_id = 'mutation_4.txt'" # 1


kill_message=$(${CLICKHOUSE_CLIENT} --query="KILL MUTATION WHERE database = 'test' AND table = 'kill_mutation' AND mutation_id = 'mutation_4.txt'")

wait

echo "$kill_message" # waiting	test	kill_mutation	mutation_4.txt	DELETE WHERE toUInt32(s) = 1

${CLICKHOUSE_CLIENT} --query="SELECT * FROM test.kill_mutation" # 2001-01-01	2	b
# must always be empty
${CLICKHOUSE_CLIENT} --query="SELECT * FROM system.mutations WHERE table = 'kill_mutation' AND database = 'test' AND is_done = 0"


${CLICKHOUSE_CLIENT} --query="DROP TABLE test.kill_mutation"
