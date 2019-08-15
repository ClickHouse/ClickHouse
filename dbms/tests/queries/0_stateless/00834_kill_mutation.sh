#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

. $CURDIR/mergetree_mutations.lib

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.kill_mutation"

${CLICKHOUSE_CLIENT} --query="CREATE TABLE test.kill_mutation(d Date, x UInt32, s String) ENGINE MergeTree ORDER BY x PARTITION BY d"

${CLICKHOUSE_CLIENT} --query="INSERT INTO test.kill_mutation VALUES ('2000-01-01', 1, 'a')"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test.kill_mutation VALUES ('2001-01-01', 2, 'b')"

${CLICKHOUSE_CLIENT} --query="SELECT '*** Create and kill a single invalid mutation ***'"

${CLICKHOUSE_CLIENT} --query="ALTER TABLE test.kill_mutation DELETE WHERE toUInt32(s) = 1"

sleep 0.1
${CLICKHOUSE_CLIENT} --query="SELECT mutation_id, latest_failed_part IN ('20000101_1_1_0', '20010101_2_2_0'), latest_fail_time != 0, substr(latest_fail_reason, 1, 8) FROM system.mutations WHERE database = 'test' AND table = 'kill_mutation'"

${CLICKHOUSE_CLIENT} --query="KILL MUTATION WHERE database = 'test' AND table = 'kill_mutation'"

${CLICKHOUSE_CLIENT} --query="SELECT mutation_id FROM system.mutations WHERE database = 'test' AND table = 'kill_mutation'"


${CLICKHOUSE_CLIENT} --query="SELECT '*** Create and kill invalid mutation that blocks another mutation ***'"

${CLICKHOUSE_CLIENT} --query="ALTER TABLE test.kill_mutation DELETE WHERE toUInt32(s) = 1"
${CLICKHOUSE_CLIENT} --query="ALTER TABLE test.kill_mutation DELETE WHERE x = 1"

${CLICKHOUSE_CLIENT} --query="SELECT mutation_id, latest_failed_part IN ('20000101_1_1_0', '20010101_2_2_0'), latest_fail_time != 0, substr(latest_fail_reason, 1, 8) FROM system.mutations WHERE database = 'test' AND table = 'kill_mutation' AND mutation_id = 'mutation_4.txt'"

sleep 0.1
${CLICKHOUSE_CLIENT} --query="KILL MUTATION WHERE database = 'test' AND table = 'kill_mutation' AND mutation_id = 'mutation_4.txt'"

wait_for_mutation "kill_mutation" "mutation_5.txt" "test"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM test.kill_mutation"


${CLICKHOUSE_CLIENT} --query="DROP TABLE test.kill_mutation"
