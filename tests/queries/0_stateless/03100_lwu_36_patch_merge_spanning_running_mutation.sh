#!/usr/bin/env bash
# Tags: no-parallel, no-shared-merge-tree
# no-parallel: waits on mt_mutate_task_pause_in_prepare, which is PAUSEABLE_ONCE and server wide,
# so a concurrent test's mutation would consume the pause this test waits for
# no-shared-merge-tree: the ENGINE = MergeTree DDL is rewritten to SharedMergeTree, whose merge
# predicate is not the MergeTreeMergePredicate whose fix this test measures

# A mutate task that has already been selected owns a data version that no part carries yet, and
# KILL MUTATION removes the mutation entry while that task keeps running. The merge predicate of
# plain MergeTree used to see neither, so it allowed a merge of patch parts across that version;
# the task then built its mutations snapshot at it and failed with
# "Found patch part ... that intersects mutation with version ...".
# Related: https://github.com/ClickHouse/ClickHouse/issues/116047

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./mergetree_mutations.lib
. "$CURDIR"/mergetree_mutations.lib

set -e

FAILPOINT="mt_mutate_task_pause_in_prepare"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FAILPOINT" 2>/dev/null ||:
}
trap cleanup EXIT

function patch_partition_id()
{
    $CLICKHOUSE_CLIENT --query "
        SELECT DISTINCT partition_id FROM system.parts
        WHERE database = currentDatabase() AND table = 't_lwu_patch_span'
          AND active AND startsWith(partition_id, 'patch-')"
}

function count_patch_parts()
{
    $CLICKHOUSE_CLIENT --query "
        SELECT count(), max(level) FROM system.parts
        WHERE database = currentDatabase() AND table = 't_lwu_patch_span'
          AND active AND startsWith(partition_id, 'patch-')"
}

function create_table()
{
    $CLICKHOUSE_CLIENT --query "
        DROP TABLE IF EXISTS t_lwu_patch_span SYNC;

        CREATE TABLE t_lwu_patch_span (id UInt64, c1 UInt64, c2 UInt64)
        ENGINE = MergeTree ORDER BY id
        SETTINGS
            enable_block_number_column = 1,
            enable_block_offset_column = 1,
            -- Only the explicit OPTIMIZE below may merge the patch parts.
            max_bytes_to_merge_at_max_space_in_pool = 1,
            -- A refused OPTIMIZE FINAL waits for the paused mutation; keep that wait short.
            lock_acquire_timeout_for_background_operations = 1;

        INSERT INTO t_lwu_patch_span SELECT number, number, number FROM numbers(10);"
}

echo "-- control: nothing between the two patches, so they do merge"
create_table

$CLICKHOUSE_CLIENT --query "
    SET enable_lightweight_update = 1;
    UPDATE t_lwu_patch_span SET c1 = 100 WHERE id = 1;
    UPDATE t_lwu_patch_span SET c1 = 300 WHERE id = 3;"

count_patch_parts
$CLICKHOUSE_CLIENT --query "
    OPTIMIZE TABLE t_lwu_patch_span PARTITION ID '$(patch_partition_id)' FINAL
    SETTINGS optimize_throw_if_noop = 0"
count_patch_parts

echo "-- a selected mutation between the two patches keeps them apart"
create_table

$CLICKHOUSE_CLIENT --query "
    SET enable_lightweight_update = 1;
    UPDATE t_lwu_patch_span SET c1 = 100 WHERE id = 1;

    SYSTEM ENABLE FAILPOINT $FAILPOINT;
    -- Takes a data version between the two patches, then blocks inside MutateTask::prepare,
    -- before it builds the mutations snapshot at that version.
    ALTER TABLE t_lwu_patch_span UPDATE c2 = 200 WHERE id = 2;"

mutation_id=$($CLICKHOUSE_CLIENT --query "
    SELECT mutation_id FROM system.mutations
    WHERE database = currentDatabase() AND table = 't_lwu_patch_span' AND NOT is_done")

# parts_in_progress_names is read from the same map the merge predicate reads, so this observes the
# tagger entry appear. It appears when the task is selected, which is before MutateTask::prepare
# runs, so the parked thread is observed separately below.
wait_for_mutation_in_progress "t_lwu_patch_span" "$mutation_id"

if ! timeout 60 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FAILPOINT PAUSE"; then
    echo "mutate task of t_lwu_patch_span never parked at $FAILPOINT" >&2
    exit 1
fi

$CLICKHOUSE_CLIENT --query "
    SET enable_lightweight_update = 1;
    UPDATE t_lwu_patch_span SET c1 = 300 WHERE id = 3;

    -- A part whose data version is above both patches, so the running mutation's version is not
    -- the largest one collected for this partition. It has to come after both patches: a data
    -- version inside their span is refused by the pre-existing check instead.
    INSERT INTO t_lwu_patch_span SELECT number, number, number FROM numbers(10, 10);

    -- Removes the mutation entry, so the mutation-version check no longer keeps the patch parts
    -- apart. Without this the merge below is refused by that check alone and the case this test
    -- guards against is not reached.
    KILL MUTATION WHERE database = currentDatabase() AND table = 't_lwu_patch_span'
        AND mutation_id = '$mutation_id' FORMAT Null;"

count_patch_parts
$CLICKHOUSE_CLIENT --query "
    OPTIMIZE TABLE t_lwu_patch_span PARTITION ID '$(patch_partition_id)' FINAL
    SETTINGS optimize_throw_if_noop = 0"
count_patch_parts

$CLICKHOUSE_CLIENT --query "
    SYSTEM DISABLE FAILPOINT $FAILPOINT;
    DROP TABLE t_lwu_patch_span SYNC;"
