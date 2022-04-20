#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# shellcheck source=./mergetree_mutations.lib
. "$CURDIR"/mergetree_mutations.lib

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS mutations_r1"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS mutations_r2"

${CLICKHOUSE_CLIENT} --query="CREATE TABLE mutations_r1(d Date, x UInt32, s String, m MATERIALIZED x + 2) ENGINE ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/mutations', 'r1', d, intDiv(x, 10), 8192)"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE mutations_r2(d Date, x UInt32, s String, m MATERIALIZED x + 2) ENGINE ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/mutations', 'r2', d, intDiv(x, 10), 8192)"

# Test a mutation on empty table
${CLICKHOUSE_CLIENT} --query="ALTER TABLE mutations_r1 DELETE WHERE x = 1 SETTINGS mutations_sync = 2"

# Insert some data
${CLICKHOUSE_CLIENT} --query="INSERT INTO mutations_r1(d, x, s) VALUES \
    ('2000-01-01', 1, 'a')"
${CLICKHOUSE_CLIENT} --query="INSERT INTO mutations_r1(d, x, s) VALUES \
    ('2000-01-01', 2, 'b'), ('2000-01-01', 3, 'c'), ('2000-01-01', 4, 'd') \
    ('2000-02-01', 2, 'b'), ('2000-02-01', 3, 'c'), ('2000-02-01', 4, 'd')"

# Try some malformed queries that should fail validation.
${CLICKHOUSE_CLIENT} --query="ALTER TABLE mutations_r1 DELETE WHERE nonexistent = 0" 2>/dev/null || echo "Query should fail 1"
${CLICKHOUSE_CLIENT} --query="ALTER TABLE mutations_r1 DELETE WHERE d = '11'" 2>/dev/null || echo "Query should fail 2"

# Delete some values
${CLICKHOUSE_CLIENT} --query="ALTER TABLE mutations_r1 DELETE WHERE x % 2 = 1 SETTINGS mutations_sync = 2"
${CLICKHOUSE_CLIENT} --query="ALTER TABLE mutations_r1 DELETE WHERE s = 'd' SETTINGS mutations_sync = 2"
${CLICKHOUSE_CLIENT} --query="ALTER TABLE mutations_r1 DELETE WHERE m = 3 SETTINGS mutations_sync = 2"

# Insert more data
${CLICKHOUSE_CLIENT} --query="INSERT INTO mutations_r1(d, x, s) VALUES \
    ('2000-01-01', 5, 'e'), ('2000-02-01', 5, 'e')"

${CLICKHOUSE_CLIENT} --query "SYSTEM SYNC REPLICA mutations_r2"

# Check that the table contains only the data that should not be deleted.
${CLICKHOUSE_CLIENT} --query="SELECT d, x, s, m FROM mutations_r2 ORDER BY d, x"
# Check the contents of the system.mutations table.
${CLICKHOUSE_CLIENT} --query="SELECT mutation_id, command, block_numbers.partition_id, block_numbers.number, parts_to_do, is_done \
    FROM system.mutations WHERE table = 'mutations_r2' ORDER BY mutation_id"


${CLICKHOUSE_CLIENT} --query="SELECT '*** Test mutations cleaner ***'"

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS mutations_cleaner_r1"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS mutations_cleaner_r2"

# Create 2 replicas with finished_mutations_to_keep = 2
${CLICKHOUSE_CLIENT} --query="CREATE TABLE mutations_cleaner_r1(x UInt32) ENGINE ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/mutations_cleaner', 'r1') ORDER BY x SETTINGS \
    finished_mutations_to_keep = 2,
    cleanup_delay_period = 1,
    cleanup_delay_period_random_add = 0"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE mutations_cleaner_r2(x UInt32) ENGINE ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/mutations_cleaner', 'r2') ORDER BY x SETTINGS \
    finished_mutations_to_keep = 2,
    cleanup_delay_period = 1,
    cleanup_delay_period_random_add = 0"

# Insert some data
${CLICKHOUSE_CLIENT} --query="INSERT INTO mutations_cleaner_r1(x) VALUES (1), (2), (3), (4), (5)"

# Add some mutations and wait for their execution
${CLICKHOUSE_CLIENT} --query="ALTER TABLE mutations_cleaner_r1 DELETE WHERE x = 1 SETTINGS mutations_sync = 2"
${CLICKHOUSE_CLIENT} --query="ALTER TABLE mutations_cleaner_r1 DELETE WHERE x = 2 SETTINGS mutations_sync = 2"
${CLICKHOUSE_CLIENT} --query="ALTER TABLE mutations_cleaner_r1 DELETE WHERE x = 3 SETTINGS mutations_sync = 2"

# Add another mutation and prevent its execution on the second replica
${CLICKHOUSE_CLIENT} --query="SYSTEM STOP REPLICATION QUEUES mutations_cleaner_r2"
${CLICKHOUSE_CLIENT} --query="ALTER TABLE mutations_cleaner_r1 DELETE WHERE x = 4"

# Sleep for more than cleanup_delay_period
sleep 1.5

# Check that the first mutation is cleaned
${CLICKHOUSE_CLIENT} --query="SELECT mutation_id, command, is_done FROM system.mutations WHERE table = 'mutations_cleaner_r2' ORDER BY mutation_id"

${CLICKHOUSE_CLIENT} --query="DROP TABLE mutations_r1"
${CLICKHOUSE_CLIENT} --query="DROP TABLE mutations_r2"

${CLICKHOUSE_CLIENT} --query="DROP TABLE mutations_cleaner_r1"
${CLICKHOUSE_CLIENT} --query="DROP TABLE mutations_cleaner_r2"
