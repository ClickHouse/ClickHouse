#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

. "$CURDIR"/mergetree_mutations.lib

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.kill_mutation_r1"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.kill_mutation_r2"

${CLICKHOUSE_CLIENT} --query="CREATE TABLE test.kill_mutation_r1(d Date, x UInt32, s String) ENGINE ReplicatedMergeTree('/clickhouse/tables/test_00834/kill_mutation', '1') ORDER BY x PARTITION BY d"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test.kill_mutation_r2(d Date, x UInt32, s String) ENGINE ReplicatedMergeTree('/clickhouse/tables/test_00834/kill_mutation', '2') ORDER BY x PARTITION BY d"

${CLICKHOUSE_CLIENT} --query="INSERT INTO test.kill_mutation_r1 VALUES ('2000-01-01', 1, 'a')"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test.kill_mutation_r1 VALUES ('2001-01-01', 2, 'b')"


${CLICKHOUSE_CLIENT} --query="SELECT '*** Create and kill a single invalid mutation ***'"

# wrong mutation
${CLICKHOUSE_CLIENT} --query="ALTER TABLE test.kill_mutation_r1 DELETE WHERE toUInt32(s) = 1 SETTINGS mutations_sync=2" 2>&1 | grep -o "happened during execution of mutation '0000000000'" | head -n 1

${CLICKHOUSE_CLIENT} --query="SELECT count() FROM system.mutations WHERE database = 'test' AND table = 'kill_mutation_r1' AND is_done = 0"

${CLICKHOUSE_CLIENT} --query="KILL MUTATION WHERE database = 'test' AND table = 'kill_mutation_r1'"

# No active mutations exists
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM system.mutations WHERE database = 'test' AND table = 'kill_mutation_r1'"

${CLICKHOUSE_CLIENT} --query="SELECT '*** Create and kill invalid mutation that blocks another mutation ***'"

${CLICKHOUSE_CLIENT} --query="SYSTEM SYNC REPLICA test.kill_mutation_r1"
${CLICKHOUSE_CLIENT} --query="SYSTEM SYNC REPLICA test.kill_mutation_r2"

# Should be empty, but in case of problems we will see some diagnostics
${CLICKHOUSE_CLIENT} --query="SELECT * FROM system.replication_queue WHERE table like 'kill_mutation_r%'"

${CLICKHOUSE_CLIENT} --query="ALTER TABLE test.kill_mutation_r1 DELETE WHERE toUInt32(s) = 1"

# good mutation, but blocked with wrong mutation
${CLICKHOUSE_CLIENT} --query="ALTER TABLE test.kill_mutation_r1 DELETE WHERE x = 1"

check_query1="SELECT count() FROM system.mutations WHERE database = 'test' AND table = 'kill_mutation_r1' AND is_done = 0"

query_result=$($CLICKHOUSE_CLIENT --query="$check_query1" 2>&1)

while [ "$query_result" != "2" ]
do
    query_result=$($CLICKHOUSE_CLIENT --query="$check_query1" 2>&1)
    sleep 0.5
done

$CLICKHOUSE_CLIENT --query="SELECT count() FROM system.mutations WHERE database = 'test' AND table = 'kill_mutation_r1' AND mutation_id = '0000000001' AND is_done = 0"

${CLICKHOUSE_CLIENT} --query="KILL MUTATION WHERE database = 'test' AND table = 'kill_mutation_r1' AND mutation_id = '0000000001'"

${CLICKHOUSE_CLIENT} --query="SYSTEM SYNC REPLICA test.kill_mutation_r1"
${CLICKHOUSE_CLIENT} --query="SYSTEM SYNC REPLICA test.kill_mutation_r2"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM test.kill_mutation_r2"

# must be empty
${CLICKHOUSE_CLIENT} --query="SELECT * FROM system.mutations WHERE table = 'kill_mutation' AND database = 'test' AND is_done = 0"

${CLICKHOUSE_CLIENT} --query="DROP TABLE test.kill_mutation_r1"
${CLICKHOUSE_CLIENT} --query="DROP TABLE test.kill_mutation_r2"
