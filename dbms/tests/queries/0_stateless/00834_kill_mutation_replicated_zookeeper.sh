#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

. $CURDIR/mergetree_mutations.lib

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.kill_mutation_r1"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.kill_mutation_r2"

${CLICKHOUSE_CLIENT} --query="CREATE TABLE test.kill_mutation_r1(d Date, x UInt32, s String) ENGINE ReplicatedMergeTree('/clickhouse/tables/test/kill_mutation', '1') ORDER BY x PARTITION BY d"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test.kill_mutation_r2(d Date, x UInt32, s String) ENGINE ReplicatedMergeTree('/clickhouse/tables/test/kill_mutation', '2') ORDER BY x PARTITION BY d"

${CLICKHOUSE_CLIENT} --query="INSERT INTO test.kill_mutation_r1 VALUES ('2000-01-01', 1, 'a')"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test.kill_mutation_r1 VALUES ('2001-01-01', 2, 'b')"


${CLICKHOUSE_CLIENT} --query="SELECT '*** Create and kill a single invalid mutation ***'"

# wrong mutation
${CLICKHOUSE_CLIENT} --query="ALTER TABLE test.kill_mutation_r1 DELETE WHERE toUInt32(s) = 1 SETTINGS mutations_sync=2" 2>&1 | grep -o "Mutation 0000000000 was killed" &

check_query1="SELECT substr(latest_fail_reason, 1, 8) as ErrorCode FROM system.mutations WHERE database = 'test' AND table = 'kill_mutation_r1' AND ErrorCode != ''"

query_result=`$CLICKHOUSE_CLIENT --query="$check_query1" 2>&1`

while [ -z "$query_result" ]
do
    query_result=`$CLICKHOUSE_CLIENT --query="$check_query1" 2>&1`
    sleep 0.1
done

$CLICKHOUSE_CLIENT --query="SELECT mutation_id, latest_failed_part IN ('20000101_0_0_0', '20010101_0_0_0'), latest_fail_time != 0, substr(latest_fail_reason, 1, 8) FROM system.mutations WHERE database = 'test' AND table = 'kill_mutation_r1'"

${CLICKHOUSE_CLIENT} --query="KILL MUTATION WHERE database = 'test' AND table = 'kill_mutation_r1'"

wait

${CLICKHOUSE_CLIENT} --query="SELECT mutation_id FROM system.mutations WHERE database = 'test' AND table = 'kill_mutation_r1'"


${CLICKHOUSE_CLIENT} --query="SELECT '*** Create and kill invalid mutation that blocks another mutation ***'"

${CLICKHOUSE_CLIENT} --query="SYSTEM SYNC REPLICA test.kill_mutation_r1"
${CLICKHOUSE_CLIENT} --query="ALTER TABLE test.kill_mutation_r1 DELETE WHERE toUInt32(s) = 1"

# good mutation, but blocked with wrong mutation
${CLICKHOUSE_CLIENT} --query="ALTER TABLE test.kill_mutation_r1 DELETE WHERE x = 1 SETTINGS mutations_sync=2" &

check_query2="SELECT substr(latest_fail_reason, 1, 8) as ErrorCode FROM system.mutations WHERE database = 'test' AND table = 'kill_mutation_r1' AND mutation_id = '0000000001' AND ErrorCode != ''"

query_result=`$CLICKHOUSE_CLIENT --query="$check_query2" 2>&1`

while [ -z "$query_result" ]
do
    query_result=`$CLICKHOUSE_CLIENT --query="$check_query2" 2>&1`
    sleep 0.1
done

$CLICKHOUSE_CLIENT --query="SELECT mutation_id, latest_failed_part IN ('20000101_0_0_0_1', '20010101_0_0_0_1'), latest_fail_time != 0, substr(latest_fail_reason, 1, 8) FROM system.mutations WHERE database = 'test' AND table = 'kill_mutation_r1' AND mutation_id = '0000000001'"

${CLICKHOUSE_CLIENT} --query="KILL MUTATION WHERE database = 'test' AND table = 'kill_mutation_r1' AND mutation_id = '0000000001'"

wait

${CLICKHOUSE_CLIENT} --query="SELECT * FROM test.kill_mutation_r2"


${CLICKHOUSE_CLIENT} --query="DROP TABLE test.kill_mutation_r1"
${CLICKHOUSE_CLIENT} --query="DROP TABLE test.kill_mutation_r2"
