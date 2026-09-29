#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database, no-shared-merge-tree
# no-parallel -- server-wide failpoints pause the projection stage of every merge and stop mutation selection on any table.
# no-replicated-database -- the local timing of `OPTIMIZE` and `ALTER` is assumed.
# no-shared-merge-tree -- the `OPTIMIZE FINAL` retry under test is StorageMergeTree's.

# `OPTIMIZE FINAL` waits for the merges that hold parts of its partition and then selects again. A
# `RENAME COLUMN` can publish new metadata and register its mutation while it waits. The retried
# selection sees that mutation and records it as materialized in the merged part, so the merge must
# also write the part with the metadata that mutation belongs to, not the metadata read before the
# wait: otherwise the part claims the rename while it still stores the old column, and the renamed
# column reads back as defaults.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./mergetree_reservations.lib
. "$CUR_DIR"/mergetree_reservations.lib

MERGE_FP="merge_task_projection_stage_pause"
MUTATE_FP="mt_select_parts_to_mutate_max_part_size"
TABLE="t_optimize_retry_metadata"
MUTATIONS="FROM system.mutations WHERE database = currentDatabase() AND table = '$TABLE'"
ACTIVE_PARTS="FROM system.parts WHERE database = currentDatabase() AND table = '$TABLE' AND active AND NOT startsWith(partition_id, 'patch-')"
OPTIMIZE_QUERY_ID="05299_optimize_final_${CLICKHOUSE_DATABASE}"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $MERGE_FP"
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $MUTATE_FP"
    wait
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $TABLE SYNC"
}
trap cleanup EXIT

# `max_bytes_to_merge_at_max_space_in_pool = 1` keeps background merges away from these parts, while
# `OPTIMIZE ... PARTITION ... FINAL` does not apply that limit. The projection only gives a merge a
# stage to pause in; it does not read the renamed column.
$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS $TABLE SYNC;
    CREATE TABLE $TABLE (id UInt64, a UInt64, PROJECTION cnt (SELECT count())) ENGINE = MergeTree ORDER BY id
    SETTINGS max_bytes_to_merge_at_max_space_in_pool = 1, min_bytes_for_wide_part = 0;
    INSERT INTO $TABLE SELECT number, 1 FROM numbers(100);
    INSERT INTO $TABLE SELECT number + 100, 1 FROM numbers(100);
"

# The first merge holds both parts, parked in the projection stage of its merge task.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $MERGE_FP"
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE $TABLE PARTITION tuple() FINAL" &
first_pid=$!
wait_failpoint $MERGE_FP

# The rename mutation must not run on its own before the retried selection: the merge under test is
# the one that materializes it.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $MUTATE_FP"

# The second `OPTIMIZE FINAL` cannot take the parts of the running merge, so it waits for it. Its
# log line is written under the mutex that the wait releases, so once the line is visible, anything
# that takes the mutex afterwards happens while it waits.
$CLICKHOUSE_CLIENT --query_id "$OPTIMIZE_QUERY_ID" --query "OPTIMIZE TABLE $TABLE PARTITION tuple() FINAL" &
second_pid=$!
wait_for_query_result "SYSTEM FLUSH LOGS text_log; SELECT count() > 0 FROM system.text_log WHERE query_id = '$OPTIMIZE_QUERY_ID' AND message LIKE 'Waiting for currently running merges%'" "1"

# A third part, created only now that the retried selection is known to be waiting: it is above the
# reservations/watermark snapshot the first attempt took, so only a rebuild on the retry can see it.
# Without the rebuild the watermark is stale, the collector drops this part, and it is left out of
# the FINAL result.
$CLICKHOUSE_CLIENT --query "INSERT INTO $TABLE SELECT number + 200, 1 FROM numbers(100)"

$CLICKHOUSE_CLIENT --query "ALTER TABLE $TABLE RENAME COLUMN a TO b SETTINGS alter_sync = 0"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $MERGE_FP"
wait "$first_pid"
wait "$second_pid"
$CLICKHOUSE_CLIENT --query "SELECT 'active parts after OPTIMIZE:', count() $ACTIVE_PARTS"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $MUTATE_FP"
wait_for_query_result "SELECT count() $MUTATIONS AND NOT is_done" "0"
$CLICKHOUSE_CLIENT --query "SELECT 'columns:', groupArray(name) FROM (SELECT name FROM system.columns WHERE database = currentDatabase() AND table = '$TABLE' ORDER BY name)"
$CLICKHOUSE_CLIENT --query "SELECT 'rows:', count(), 'sum(b):', sum(b), 'b = 1:', countIf(b = 1) FROM $TABLE"
