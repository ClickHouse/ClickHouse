#!/usr/bin/env bash
# Tags: replica, no-debug, no-fasttest, no-shared-merge-tree
# no-fasttest: Waiting for failed mutations is slow: https://github.com/ClickHouse/ClickHouse/issues/67936
# no-shared-merge-tree: kill mutation looks different for shared merge tree, implemented another test

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# shellcheck source=./mergetree_mutations.lib
. "$CURDIR"/mergetree_mutations.lib

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS kill_mutation_r1"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS kill_mutation_r2"

${CLICKHOUSE_CLIENT} --query="CREATE TABLE kill_mutation_r1(d Date, x UInt32, s String) ENGINE ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/kill_mutation', '1') ORDER BY x PARTITION BY d"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE kill_mutation_r2(d Date, x UInt32, s String) ENGINE ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/kill_mutation', '2') ORDER BY x PARTITION BY d"

${CLICKHOUSE_CLIENT} --query="INSERT INTO kill_mutation_r1 VALUES ('2000-01-01', 1, 'a')"
${CLICKHOUSE_CLIENT} --query="INSERT INTO kill_mutation_r1 VALUES ('2001-01-01', 2, 'b')"


${CLICKHOUSE_CLIENT} --query="SELECT '*** Create and kill a single invalid mutation ***'"

# wrong mutation
${CLICKHOUSE_CLIENT} --query="ALTER TABLE kill_mutation_r1 DELETE WHERE toUInt32(s) = 1 SETTINGS mutations_sync=2" 2>&1 | grep -o "happened during execution of mutation '0000000000'" | head -n 1

${CLICKHOUSE_CLIENT} --query="SELECT count() FROM system.mutations WHERE database = '$CLICKHOUSE_DATABASE' AND table = 'kill_mutation_r1' AND is_done = 0"

${CLICKHOUSE_CLIENT} --query="KILL MUTATION WHERE database = '$CLICKHOUSE_DATABASE' AND table = 'kill_mutation_r1'"

# No active mutations exists
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM system.mutations WHERE database = '$CLICKHOUSE_DATABASE' AND table = 'kill_mutation_r1'"

${CLICKHOUSE_CLIENT} --query="SELECT '*** Create and kill invalid mutation that blocks another mutation ***'"

${CLICKHOUSE_CLIENT} --query="SYSTEM SYNC REPLICA kill_mutation_r1"
${CLICKHOUSE_CLIENT} --query="SYSTEM SYNC REPLICA kill_mutation_r2"

# Should be empty, but in case of problems we will see some diagnostics
${CLICKHOUSE_CLIENT} --query="SELECT * FROM system.replication_queue WHERE database = '$CLICKHOUSE_DATABASE' AND table like 'kill_mutation_r%'"

${CLICKHOUSE_CLIENT} --query="ALTER TABLE kill_mutation_r1 DELETE WHERE toUInt32(s) = 1"

# good mutation, but blocked with wrong mutation
${CLICKHOUSE_CLIENT} --query="ALTER TABLE kill_mutation_r1 DELETE WHERE x = 1"

check_query1="SELECT count() FROM system.mutations WHERE database = '$CLICKHOUSE_DATABASE' AND table = 'kill_mutation_r1' AND is_done = 0"

query_result=$($CLICKHOUSE_CLIENT --query="$check_query1" 2>&1)

while [ "$query_result" != "2" ]
do
    query_result=$($CLICKHOUSE_CLIENT --query="$check_query1" 2>&1)
    sleep 0.5
done

$CLICKHOUSE_CLIENT --query="SELECT count() FROM system.mutations WHERE database = '$CLICKHOUSE_DATABASE' AND table = 'kill_mutation_r1' AND mutation_id = '0000000001' AND is_done = 0"

${CLICKHOUSE_CLIENT} --query="KILL MUTATION WHERE database = '$CLICKHOUSE_DATABASE' AND table = 'kill_mutation_r1' AND mutation_id = '0000000001'"

# Wait for the 1st mutation to be actually killed and the 2nd to finish
query_result=$($CLICKHOUSE_CLIENT --query="$check_query1" 2>&1)
while [ "$query_result" != "0" ]
do
    query_result=$($CLICKHOUSE_CLIENT --query="$check_query1" 2>&1)
    sleep 0.5
done

${CLICKHOUSE_CLIENT} --query="SYSTEM SYNC REPLICA kill_mutation_r1"
${CLICKHOUSE_CLIENT} --query="SYSTEM SYNC REPLICA kill_mutation_r2"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM kill_mutation_r2"

# must be empty
${CLICKHOUSE_CLIENT} --query="SELECT * FROM system.mutations WHERE table = 'kill_mutation' AND database = '$CLICKHOUSE_DATABASE' AND is_done = 0"

${CLICKHOUSE_CLIENT} --query="DROP TABLE kill_mutation_r1"
${CLICKHOUSE_CLIENT} --query="DROP TABLE kill_mutation_r2"
