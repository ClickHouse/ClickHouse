#!/usr/bin/env bash
# Tags: replica, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

. "$CURDIR"/../shell_config.sh

# Drop existing tables if they exist.
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test_mutations_kill SYNC"

# Test: Mutations Kill

${CLICKHOUSE_CLIENT} --query="""
CREATE TABLE test_mutations_kill
(
    c1 UInt64,
    c2 String,
    c3 UInt64,
    c4 UInt64
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/test_mutations_kill','1')
ORDER BY (c1, c2, c3);
"""

# Insert 4 million rows into test_mutations_kill.
${CLICKHOUSE_CLIENT} --query="""
INSERT INTO test_mutations_kill
SELECT
    number AS c1,
    concat('string_', toString(number)) AS c2,
    number % 100 AS c3,
    number * 2 AS c4
FROM numbers(4000000)
SETTINGS min_insert_block_size_rows = 4000000, min_insert_block_size_bytes = 0;
"""

# Apply a mutation that deletes rows; the sleepEachRow function slightly delays processing.
${CLICKHOUSE_CLIENT} --query="ALTER TABLE test_mutations_kill DELETE WHERE c1 > sleepEachRow(0.00001)"

# Wait until the mutation is in progress.
for i in {1..100}; do
    # Check if the mutation merge started data reading (rows_read > 0), the cancelling didn't work at that phase before the fix in PR#77766
    mutation_count=$(${CLICKHOUSE_CLIENT} --query="SELECT count() FROM system.merges WHERE table = 'test_mutations_kill' AND database = '$CLICKHOUSE_DATABASE' AND is_mutation AND rows_read > 0")

    if [[ "$mutation_count" -gt 0 ]]; then
        echo "Mutation started."
        break
    fi

    if [[ "$i" -eq 100 ]]; then
        echo "Mutation did not start in time."
        exit 1
    fi

    sleep 0.1
done

# Note: The 'KILL MUTATION' can't be used in that test, because it only assigns a new 'fake' mutation which will make the killed one no-op, but it does not stop the mutation of parts already in progress.
# ${CLICKHOUSE_CLIENT} --query="KILL MUTATION WHERE table = 'test_mutations_kill' AND database = '$CLICKHOUSE_DATABASE' FORMAT Vertical"

# Stop merges for test_mutations_kill to cancel the mutation merge.
${CLICKHOUSE_CLIENT} --query="SYSTEM STOP MERGES test_mutations_kill"

# Polling loop with a timeout to ensure the mutation merge has stopped.
for i in {1..50}; do
    active_merges=$(${CLICKHOUSE_CLIENT} --query="SELECT count() FROM system.merges WHERE table = 'test_mutations_kill' AND database = '$CLICKHOUSE_DATABASE' AND is_mutation")

    if [[ "$active_merges" -eq 0 ]]; then
        echo "Mutation merge finished or cancelled."
        break
    fi

    if [[ "$i" -eq 50 ]]; then
        echo "Timeout: Mutation still active in system.merges after waiting."
        exit 1
    fi

    sleep 0.5
done

${CLICKHOUSE_CLIENT} --query="SYSTEM FLUSH LOGS"

# Check the part log for mutation cancellation status.
${CLICKHOUSE_CLIENT} --query="""
SELECT if(read_rows < 4000000 AND error = 236 AND exception LIKE '%Cancelled mutating parts%', 'OK',
       format('FAIL! read_rows: {}, error: {}, exception: {}', read_rows, error, exception))
FROM system.part_log
WHERE table = 'test_mutations_kill'
  AND database = '$CLICKHOUSE_DATABASE'
  AND event_time > now() - INTERVAL 3 MINUTE
  AND event_type = 'MutatePart'
ORDER BY event_time FORMAT TSVRaw;
"""

# Drop the test_mutations_kill table.
${CLICKHOUSE_CLIENT} --query="DROP TABLE test_mutations_kill SYNC"

# Test: Merges Kill

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test_merges_kill SYNC"

${CLICKHOUSE_CLIENT} --query="""
CREATE TABLE test_merges_kill
(
    c1 UInt64,
    c2 UInt64
)
ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/test_merges_kill','1')
ORDER BY (c1);
"""

# Insert 4 million rows into test_merges_kill.
# After merging, these rows will be aggregated to 0 rows.
${CLICKHOUSE_CLIENT} --query="""
INSERT INTO test_merges_kill
SELECT
    1 AS c1,
    0 AS c2
FROM numbers(4000000)
SETTINGS min_insert_block_size_rows = 4000000, min_insert_block_size_bytes = 0, optimize_on_insert = 0;
"""

# Add a materialized column to slow down the subsequent merge.
${CLICKHOUSE_CLIENT} --query="ALTER TABLE test_merges_kill ADD COLUMN c3 UInt64 MATERIALIZED sleepEachRow(0.00001)"

# Trigger the merge by optimizing the table.
${CLICKHOUSE_CLIENT} --query="OPTIMIZE TABLE test_merges_kill FINAL SETTINGS alter_sync = 0"

# Wait until the merge is in progress.
for i in {1..100}; do
    merge_count=$(${CLICKHOUSE_CLIENT} --query="SELECT count() FROM system.merges WHERE table = 'test_merges_kill' AND database = '$CLICKHOUSE_DATABASE' AND rows_read > 0")

    if [[ "$merge_count" -gt 0 ]]; then
        echo "Merge started."
        break
    fi

    if [[ "$i" -eq 100 ]]; then
        echo "Merge did not start in time."
        break
    fi

    sleep 0.1
done

# Stop merges for test_merges_kill to cancel the merge.
${CLICKHOUSE_CLIENT} --query="SYSTEM STOP MERGES test_merges_kill"


# Polling loop with a timeout to ensure the merge has stopped.
for i in {1..50}; do
    active_merges=$(${CLICKHOUSE_CLIENT} --query="SELECT count() FROM system.merges WHERE table = 'test_merges_kill' AND database = '$CLICKHOUSE_DATABASE'")

    if [[ "$active_merges" -eq 0 ]]; then
        echo "Merge finished or cancelled."
        break
    fi

    if [[ "$i" -eq 50 ]]; then
        echo "Timeout: Merge still active after waiting."
        break
    fi

    sleep 0.5
done

${CLICKHOUSE_CLIENT} --query="SYSTEM FLUSH LOGS"

# Check the part log for merge cancellation status.
${CLICKHOUSE_CLIENT} --query="""
SELECT if(read_rows < 4000000 AND error = 236 AND exception LIKE '%Cancelled merging parts%', 'OK',
       format('FAIL! read_rows: {}, error: {}, exception: {}', read_rows, error, exception))
FROM system.part_log
WHERE table = 'test_merges_kill'
  AND database = '$CLICKHOUSE_DATABASE'
  AND event_time > now() - INTERVAL 3 MINUTE
  AND event_type = 'MergeParts'
ORDER BY event_time FORMAT TSVRaw;
"""


# Drop the test_merges_kill table.
${CLICKHOUSE_CLIENT} --query="DROP TABLE test_merges_kill SYNC"
