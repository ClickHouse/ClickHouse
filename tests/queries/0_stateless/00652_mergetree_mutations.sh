#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# shellcheck source=./mergetree_mutations.lib
. "$CURDIR"/mergetree_mutations.lib

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS mutations"

${CLICKHOUSE_CLIENT} --allow_deprecated_syntax_for_merge_tree=1 --query="CREATE TABLE mutations(d Date, x UInt32, s String, a UInt32 ALIAS x + 1, m MATERIALIZED x + 2) ENGINE MergeTree(d, intDiv(x, 10), 8192)"

# Test a mutation on empty table
${CLICKHOUSE_CLIENT} --query="ALTER TABLE mutations DELETE WHERE x = 1"

# Insert some data
${CLICKHOUSE_CLIENT} --query="INSERT INTO mutations(d, x, s) VALUES \
    ('2000-01-01', 1, 'a')"
${CLICKHOUSE_CLIENT} --query="INSERT INTO mutations(d, x, s) VALUES \
    ('2000-01-01', 2, 'b'), ('2000-01-01', 3, 'c'), ('2000-01-01', 4, 'd') \
    ('2000-02-01', 2, 'b'), ('2000-02-01', 3, 'c'), ('2000-02-01', 4, 'd')"

# Try some malformed queries that should fail validation.
${CLICKHOUSE_CLIENT} --query="ALTER TABLE mutations DELETE WHERE nonexistent = 0" 2>/dev/null || echo "Query should fail 1"
${CLICKHOUSE_CLIENT} --query="ALTER TABLE mutations DELETE WHERE d = '11'" 2>/dev/null || echo "Query should fail 2"
# TODO: Queries involving alias columns are not supported yet and should fail on submission.
${CLICKHOUSE_CLIENT} --query="ALTER TABLE mutations UPDATE s = s || '' WHERE a = 0" 2>/dev/null || echo "Query involving aliases should fail on submission"

# Delete some values
${CLICKHOUSE_CLIENT} --query="ALTER TABLE mutations DELETE WHERE x % 2 = 1"
${CLICKHOUSE_CLIENT} --query="ALTER TABLE mutations DELETE WHERE s = 'd'"
${CLICKHOUSE_CLIENT} --query="ALTER TABLE mutations DELETE WHERE m = 3"

# Insert more data
${CLICKHOUSE_CLIENT} --query="INSERT INTO mutations(d, x, s) VALUES \
    ('2000-01-01', 5, 'e'), ('2000-02-01', 5, 'e')"

# Wait until the last mutation is done.
wait_for_mutation "mutations" "mutation_7.txt"

# Check that the table contains only the data that should not be deleted.
${CLICKHOUSE_CLIENT} --query="SELECT d, x, s, m FROM mutations ORDER BY d, x"
# Check the contents of the system.mutations table.
${CLICKHOUSE_CLIENT} --query="SELECT mutation_id, command, block_numbers.partition_id, block_numbers.number, parts_to_do, is_done \
    FROM system.mutations WHERE database = '$CLICKHOUSE_DATABASE' and table = 'mutations' ORDER BY mutation_id"

${CLICKHOUSE_CLIENT} --query="DROP TABLE mutations"


${CLICKHOUSE_CLIENT} --query="SELECT '*** Test mutations cleaner ***'"

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS mutations_cleaner"

# Create a table with finished_mutations_to_keep = 2
${CLICKHOUSE_CLIENT} --query="CREATE TABLE mutations_cleaner(x UInt32) ENGINE MergeTree ORDER BY x SETTINGS finished_mutations_to_keep = 2"

# Insert some data
${CLICKHOUSE_CLIENT} --query="INSERT INTO mutations_cleaner(x) VALUES (1), (2), (3), (4)"

# Add some mutations and wait for their execution
${CLICKHOUSE_CLIENT} --query="ALTER TABLE mutations_cleaner DELETE WHERE x = 1"
${CLICKHOUSE_CLIENT} --query="ALTER TABLE mutations_cleaner DELETE WHERE x = 2"
${CLICKHOUSE_CLIENT} --query="ALTER TABLE mutations_cleaner DELETE WHERE x = 3"

wait_for_mutation "mutations_cleaner" "mutation_4.txt"

# Sleep and then do an INSERT to wakeup the background task that will clean up the old mutations
sleep 1
${CLICKHOUSE_CLIENT} --query="INSERT INTO mutations_cleaner(x) VALUES (4)"
sleep 0.1

# Check that the first mutation is cleaned
${CLICKHOUSE_CLIENT} --query="SELECT mutation_id, command, is_done FROM system.mutations WHERE database = '$CLICKHOUSE_DATABASE' and table = 'mutations_cleaner' ORDER BY mutation_id"

${CLICKHOUSE_CLIENT} --query="DROP TABLE mutations_cleaner"
