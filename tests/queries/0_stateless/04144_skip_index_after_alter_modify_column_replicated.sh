#!/usr/bin/env bash
# Tags: zookeeper

# Test that a read cannot use a skip index whose on-disk data was serialized with the old column
# type while an `ALTER MODIFY COLUMN` that changes that type is still pending (ReplicatedMergeTree
# variant). Evaluating the old index data under the new type prunes granules that do match,
# returning 0 rows instead of 128. Both phases of skip-index application refuse such an index per
# part through `MergeTreeDataSelectExecutor::canUseIndex`, and `supportsSkipIndexesOnDataRead`
# keeps the data-read phase out of this state as well, so the read below asserts the outcome and
# not which of them produced it.
#
# Uses UInt64 → Float64 conversion because both types use 8-byte fixed-width
# serialization: the old UInt64 bytes get reinterpreted as Float64 without a
# deserialization error, producing tiny denormalized values that silently cause
# the set index to incorrectly skip granules (returning 0 rows instead of 128).
#
# `max_rows_to_read = 0` is needed to enable the data-read phase in
# `supportsSkipIndexesOnDataRead` (it disables itself when `read_overflow_mode = throw`
# and `max_rows_to_read` is set, which `clickhouse-test` injects by default).
#
# On ReplicatedMergeTree one setting (`alter_sync`) gates the `ALTER`'s metadata wait and its
# mutation wait together, so no value of it leaves the pending state observable while also
# reaching the applied state, and no later statement waits for a submitted mutation. Both states
# are therefore reached by polling `system.columns` / `system.mutations`, which is why this is a
# shell script.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./mergetree_mutations.lib
. "$CUR_DIR"/mergetree_mutations.lib

${CLICKHOUSE_CLIENT} -q "
DROP TABLE IF EXISTS test_skip_index_alter_replicated;

CREATE TABLE test_skip_index_alter_replicated
(
    id UInt64,
    value UInt64,
    INDEX idx_value (value) TYPE set(0) GRANULARITY 1
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_skip_index_alter_replicated', '1')
ORDER BY id
SETTINGS index_granularity = 128;

-- Insert enough rows across multiple granules so the skip index is populated and used.
-- Granule 0 (rows 0..127) has value 200, granule 1 (rows 128..255) has value 300.
INSERT INTO test_skip_index_alter_replicated SELECT number, if(number < 128, 200, 300) FROM numbers(256);

-- Verify initial index usage works
SELECT count() FROM test_skip_index_alter_replicated WHERE value = 300 SETTINGS force_data_skipping_indices = 'idx_value', use_skip_indexes_on_data_read = 1, max_rows_to_read = 0;

-- Stop merges so the mutation doesn't get applied
SYSTEM STOP MERGES test_skip_index_alter_replicated;
"

# Change column type; creates an ALTER mutation (READ_COLUMN), not a data mutation.
# `MODIFY COLUMN` waits according to `alter_sync`, so 0 is what makes it asynchronous.
${CLICKHOUSE_CLIENT} --alter_sync=0 -q "ALTER TABLE test_skip_index_alter_replicated MODIFY COLUMN value Float64"

# At `alter_sync = 0` the ALTER waits for neither the metadata change nor its mutation, so wait
# for the state the read below is about: the new type visible on this replica, mutation still
# pending. Merges are stopped, so ALTER_METADATA is executed while MUTATE_PART stays postponed,
# which makes both halves of the condition monotone once they hold.
for _ in {1..300}
do
    pending=$(${CLICKHOUSE_CLIENT} -q "SELECT (SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'test_skip_index_alter_replicated' AND name = 'value') = 'Float64' AND (SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 'test_skip_index_alter_replicated' AND NOT is_done) > 0")
    [[ "$pending" == "1" ]] && break
    sleep 0.3
done
echo "$pending"

${CLICKHOUSE_CLIENT} -q "
-- The on-disk index data is incompatible with the new type, so no path may use it to prune:
-- read as Float64, the old UInt64 bytes become tiny denormalized values that do not match
-- 300.0, which would skip every granule and return 0 rows instead of 128.
SELECT count() FROM test_skip_index_alter_replicated WHERE value = 300.0 SETTINGS force_data_skipping_indices = 'idx_value', use_skip_indexes_on_data_read = 1, max_rows_to_read = 0;

SYSTEM START MERGES test_skip_index_alter_replicated;
"

wait_for_all_mutations "test_skip_index_alter_replicated"

${CLICKHOUSE_CLIENT} -q "
-- Both states return 128, so this count is what pins which of them the assertion below runs in.
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 'test_skip_index_alter_replicated' AND NOT is_done;

-- After mutation completes, the index should work with the new type
SELECT count() FROM test_skip_index_alter_replicated WHERE value = 300.0 SETTINGS force_data_skipping_indices = 'idx_value', use_skip_indexes_on_data_read = 1, max_rows_to_read = 0;

DROP TABLE test_skip_index_alter_replicated;
"
