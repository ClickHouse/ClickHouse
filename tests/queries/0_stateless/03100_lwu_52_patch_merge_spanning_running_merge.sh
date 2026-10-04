#!/usr/bin/env bash
# Tags: no-parallel, no-shared-merge-tree
# no-parallel: enables mt_merge_task_pause_in_prepare_with_patches, which is server wide, so while
# this test runs it would also hold a concurrent test's merge of patch parts
# no-shared-merge-tree: the ENGINE = MergeTree DDL is rewritten to SharedMergeTree, which uses
# neither MergePlainMergeTreeTask, so the failpoint never fires, nor the merge predicate under test

# A merge that applies patch parts takes the data version of its result from those patches, so while
# that merge runs the version is on no active part. The merge predicate of plain MergeTree used to
# collect versions from the active parts only, so it allowed a merge of patch parts across that
# version; the merge then committed its result at exactly that version, leaving a patch that spans
# the data version of a live part, so assertNoPatchesForParts throws for it and DETACH PART, DETACH
# PARTITION, MOVE PARTITION and REPLACE PARTITION ... FROM all abort with
# "Found patch part ... that intersects mutation with version ...".
# The sibling test 03100_lwu_36 covers the other source of such a version, a running mutate task,
# which instead fails when that task builds its mutations snapshot at its own version.
# Related: https://github.com/ClickHouse/ClickHouse/issues/116047

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

FAILPOINT="mt_merge_task_pause_in_prepare_with_patches"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FAILPOINT" 2>/dev/null ||:
}
trap cleanup EXIT

function patch_partition_id()
{
    $CLICKHOUSE_CLIENT --query "
        SELECT DISTINCT partition_id FROM system.parts
        WHERE database = currentDatabase() AND table = 't_lwu_merge_span'
          AND active AND startsWith(partition_id, 'patch-')"
}

function count_patch_parts()
{
    $CLICKHOUSE_CLIENT --query "
        SELECT count(), max(level) FROM system.parts
        WHERE database = currentDatabase() AND table = 't_lwu_merge_span'
          AND active AND startsWith(partition_id, 'patch-')"
}

function count_running_merges()
{
    $CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.merges
        WHERE database = currentDatabase() AND table = 't_lwu_merge_span' AND NOT is_mutation"
}

function create_table()
{
    $CLICKHOUSE_CLIENT --query "
        DROP TABLE IF EXISTS t_lwu_merge_span SYNC;

        CREATE TABLE t_lwu_merge_span (id UInt64, c1 UInt64, c2 UInt64)
        ENGINE = MergeTree ORDER BY id
        SETTINGS
            enable_block_number_column = 1,
            enable_block_offset_column = 1,
            -- The merge below has to apply the patch part, that is what raises its result version.
            apply_patches_on_merge = 1,
            -- Only the explicit OPTIMIZE statements below may merge anything.
            max_bytes_to_merge_at_max_space_in_pool = 1,
            -- A refused OPTIMIZE FINAL waits for the paused merge; keep that wait short.
            lock_acquire_timeout_for_background_operations = 1;

        INSERT INTO t_lwu_merge_span SELECT number, number, number FROM numbers(10);"
}

# Both arms end with a part whose data version is above both patches, so that the version appended by
# the predicate is not the largest one collected for the partition and the order it restores matters.
# The part has to come after both patches: a data version inside their span is refused by the
# pre-existing check for the versions of the visible parts instead.
function insert_part_above_patches()
{
    $CLICKHOUSE_CLIENT --query "INSERT INTO t_lwu_merge_span SELECT number, number, number FROM numbers(10, 10)"
}

echo "-- control: nothing between the two patches, so they do merge"
create_table

$CLICKHOUSE_CLIENT --query "
    SET enable_lightweight_update = 1;
    UPDATE t_lwu_merge_span SET c1 = 100 WHERE id = 1;
    UPDATE t_lwu_merge_span SET c1 = 300 WHERE id = 3;"

insert_part_above_patches

count_running_merges
count_patch_parts
$CLICKHOUSE_CLIENT --query "
    OPTIMIZE TABLE t_lwu_merge_span PARTITION ID '$(patch_partition_id)' FINAL
    SETTINGS optimize_throw_if_noop = 0"
count_patch_parts

echo "-- a merge that applies the first patch keeps the two patches apart"
create_table

$CLICKHOUSE_CLIENT --query "
    SET enable_lightweight_update = 1;
    UPDATE t_lwu_merge_span SET c1 = 100 WHERE id = 1;

    SYSTEM ENABLE FAILPOINT $FAILPOINT;"

# Applies the patch above, so the result part takes that patch's data version, and blocks in
# MergePlainMergeTreeTask::prepare before committing it. In the background because
# StorageMergeTree::merge runs the merge inline, so the statement returns only once it is released.
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE t_lwu_merge_span PARTITION ID 'all' FINAL SETTINGS optimize_throw_if_noop = 1" &
optimize_pid=$!

# Wait for THIS table's merge to reach the merge list, then for a thread to actually park at the
# failpoint. Both are observed states rather than a sleep, and once the merge is in the list it
# cannot commit, because it has to pass the failpoint first.
merge_started=0
for _ in {1..600}; do
    [[ "$(count_running_merges)" == "1" ]] && { merge_started=1; break; }
    sleep 0.1
done

if [[ "$merge_started" != "1" ]]; then
    echo "no merge of t_lwu_merge_span appeared in system.merges" >&2
    exit 1
fi

$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FAILPOINT PAUSE"

$CLICKHOUSE_CLIENT --query "
    SET enable_lightweight_update = 1;
    UPDATE t_lwu_merge_span SET c1 = 300 WHERE id = 3;"

insert_part_above_patches

# The state under test: the merge is still in flight, so its result version is on no part yet.
count_running_merges
count_patch_parts
$CLICKHOUSE_CLIENT --query "
    OPTIMIZE TABLE t_lwu_merge_span PARTITION ID '$(patch_partition_id)' FINAL
    SETTINGS optimize_throw_if_noop = 0"
count_patch_parts

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FAILPOINT"
wait $optimize_pid

# The merge committed its result at the first patch's data version, so that patch is applied to the
# result and the second one is not yet. A patch wrongly applied or skipped changes these values; the
# merge decision above, not these rows, is what catches a patch merged across the version.
$CLICKHOUSE_CLIENT --query "
    SELECT id, c1 FROM t_lwu_merge_span WHERE id IN (1, 3) ORDER BY id SETTINGS apply_patch_parts = 1;
    SELECT id, c1 FROM t_lwu_merge_span WHERE id IN (1, 3) ORDER BY id SETTINGS apply_patch_parts = 0;

    DROP TABLE t_lwu_merge_span SYNC;"
