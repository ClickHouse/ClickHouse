#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

. $CURDIR/mergetree_mutations.lib

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.mutations_r1"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.mutations_r2"

${CLICKHOUSE_CLIENT} --query="CREATE TABLE test.mutations_r1(d Date, x UInt32, s String, m MATERIALIZED x + 2) ENGINE ReplicatedMergeTree('/clickhouse/tables/test/mutations', 'r1', d, intDiv(x, 10), 8192)"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test.mutations_r2(d Date, x UInt32, s String, m MATERIALIZED x + 2) ENGINE ReplicatedMergeTree('/clickhouse/tables/test/mutations', 'r2', d, intDiv(x, 10), 8192)"

# Test a mutation on empty table
${CLICKHOUSE_CLIENT} --query="ALTER TABLE test.mutations_r1 DELETE WHERE x = 1"

# Insert some data
${CLICKHOUSE_CLIENT} --query="INSERT INTO test.mutations_r1(d, x, s) VALUES \
    ('2000-01-01', 1, 'a')"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test.mutations_r1(d, x, s) VALUES \
    ('2000-01-01', 2, 'b'), ('2000-01-01', 3, 'c'), ('2000-01-01', 4, 'd') \
    ('2000-02-01', 2, 'b'), ('2000-02-01', 3, 'c'), ('2000-02-01', 4, 'd')"

# Try some malformed queries that should fail validation.
${CLICKHOUSE_CLIENT} --query="ALTER TABLE test.mutations_r1 DELETE WHERE nonexistent = 0" 2>/dev/null || echo "Query should fail 1"
${CLICKHOUSE_CLIENT} --query="ALTER TABLE test.mutations_r1 DELETE WHERE d = '11'" 2>/dev/null || echo "Query should fail 2"

# Delete some values
${CLICKHOUSE_CLIENT} --query="ALTER TABLE test.mutations_r1 DELETE WHERE x % 2 = 1"
${CLICKHOUSE_CLIENT} --query="ALTER TABLE test.mutations_r1 DELETE WHERE s = 'd'"
${CLICKHOUSE_CLIENT} --query="ALTER TABLE test.mutations_r1 DELETE WHERE m = 3"

# Insert more data
${CLICKHOUSE_CLIENT} --query="INSERT INTO test.mutations_r1(d, x, s) VALUES \
    ('2000-01-01', 5, 'e'), ('2000-02-01', 5, 'e')"

# Wait until the last mutation is done.
wait_for_mutation "mutations_r2" "0000000003"

# Check that the table contains only the data that should not be deleted.
${CLICKHOUSE_CLIENT} --query="SELECT d, x, s, m FROM test.mutations_r2 ORDER BY d, x"
# Check the contents of the system.mutations table.
${CLICKHOUSE_CLIENT} --query="SELECT mutation_id, command, block_numbers.partition_id, block_numbers.number, parts_to_do, is_done \
    FROM system.mutations WHERE table = 'mutations_r2' ORDER BY mutation_id"


${CLICKHOUSE_CLIENT} --query="SELECT '*** Test mutations cleaner ***'"

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.mutations_cleaner_r1"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.mutations_cleaner_r2"

# Create 2 replicas with finished_mutations_to_keep = 2
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test.mutations_cleaner_r1(x UInt32) ENGINE ReplicatedMergeTree('/clickhouse/tables/test/mutations_cleaner', 'r1') ORDER BY x SETTINGS \
    finished_mutations_to_keep = 2,
    cleanup_delay_period = 1,
    cleanup_delay_period_random_add = 0"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test.mutations_cleaner_r2(x UInt32) ENGINE ReplicatedMergeTree('/clickhouse/tables/test/mutations_cleaner', 'r2') ORDER BY x SETTINGS \
    finished_mutations_to_keep = 2,
    cleanup_delay_period = 1,
    cleanup_delay_period_random_add = 0"

# Insert some data
${CLICKHOUSE_CLIENT} --query="INSERT INTO test.mutations_cleaner_r1(x) VALUES (1), (2), (3), (4)"

# Add some mutations and wait for their execution
${CLICKHOUSE_CLIENT} --query="ALTER TABLE test.mutations_cleaner_r1 DELETE WHERE x = 1"
${CLICKHOUSE_CLIENT} --query="ALTER TABLE test.mutations_cleaner_r1 DELETE WHERE x = 2"
${CLICKHOUSE_CLIENT} --query="ALTER TABLE test.mutations_cleaner_r1 DELETE WHERE x = 3"

wait_for_mutation "mutations_cleaner_r2" "0000000002"

# Add another mutation and prevent its execution on the second replica
${CLICKHOUSE_CLIENT} --query="SYSTEM STOP REPLICATION QUEUES test.mutations_cleaner_r2"
${CLICKHOUSE_CLIENT} --query="ALTER TABLE test.mutations_cleaner_r1 DELETE WHERE x = 4"

# Sleep for more than cleanup_delay_period
sleep 1.5

# Check that the first mutation is cleaned
${CLICKHOUSE_CLIENT} --query="SELECT mutation_id, command, is_done FROM system.mutations WHERE table = 'mutations_cleaner_r2' ORDER BY mutation_id"

${CLICKHOUSE_CLIENT} --query="DROP TABLE test.mutations_r1"
${CLICKHOUSE_CLIENT} --query="DROP TABLE test.mutations_r2"

${CLICKHOUSE_CLIENT} --query="DROP TABLE test.mutations_cleaner_r1"
${CLICKHOUSE_CLIENT} --query="DROP TABLE test.mutations_cleaner_r2"
