#!/usr/bin/env bash
# Tags: no-shared-catalog
# no-shared-catalog: STOP MERGES only stops them on the current replica, so the second one can
# materialize the UPDATE separately and the two mutations never reach one mutate pass

# `CLEAR COLUMN` recomputes the MATERIALIZED columns derived from the cleared one. The set of columns
# the mutation writes used to be frozen before the recompute stages were appended, whenever anything
# had already put a stage in place. A still-pending `ALTER UPDATE` that coalesces with the `CLEAR`
# into one mutate pass does exactly that - at pure defaults, with no block column involved - so this
# case pins a trigger `04869_clear_column_materialized_with_block_columns` cannot reach.
#
# Bash rather than SQL because the two commands have to be pending together when the pass runs:
# merges are stopped, both mutations are issued asynchronously, merges are resumed, and only a poll
# on `system.mutations` can tell when the pass finished. The same two commands applied as separate
# passes give the right answer even unfixed, so the test asserts the coalescing too - otherwise it
# would pass vacuously whenever the pass happened to split.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./mergetree_mutations.lib
. "$CUR_DIR"/mergetree_mutations.lib

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_clear_coalesced_mutation"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_clear_coalesced_mutation (x Int32, y Int32, z Int32, mk Int32 MATERIALIZED x + 1)
    ENGINE = MergeTree ORDER BY tuple() PARTITION BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
             enable_block_number_column = 0, enable_block_offset_column = 0"

$CLICKHOUSE_CLIENT -q "INSERT INTO t_clear_coalesced_mutation (x, y, z) VALUES (1, 0, 7)"
$CLICKHOUSE_CLIENT -q "SELECT 'before', x, y, z, mk FROM t_clear_coalesced_mutation"

# Lower bound for the `part_log` assertion below, so a rerun in a pinned database cannot count the
# rows of an earlier invocation. Microsecond precision is needed to tell them from a row written by
# a previous run within the same second.
run_start=$($CLICKHOUSE_CLIENT -q "SELECT toUnixTimestamp64Micro(now64(6))")

$CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES t_clear_coalesced_mutation"
# `alter_sync`, not `mutations_sync`, is what `CLEAR COLUMN` waits on: it reaches
# `StorageMergeTree::alter`, which blocks on `alter_sync` (default 1) while merges are stopped.
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_clear_coalesced_mutation UPDATE y = 10 WHERE 1 SETTINGS mutations_sync = 0, alter_sync = 0"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_clear_coalesced_mutation CLEAR COLUMN x IN PARTITION tuple() SETTINGS mutations_sync = 0, alter_sync = 0"
$CLICKHOUSE_CLIENT -q "SYSTEM START MERGES t_clear_coalesced_mutation"

wait_for_all_mutations "t_clear_coalesced_mutation"

# `mk` must follow the cleared `x` down to its default, and `y` must carry the update.
$CLICKHOUSE_CLIENT -q "SELECT 'after', x, y, z, mk FROM t_clear_coalesced_mutation"

# One MutatePart for both commands, i.e. they really were applied in one pass. Without this the
# assertion above is vacuous: two separate passes produce the right value even on unfixed code.
#
# `wait_for_all_mutations` only waits for `system.mutations.is_done`, which does not mean the
# `MutatePart` row has reached `system.part_log` yet, so poll instead of reading once after a single
# flush - the same reason `03221_mutate_profile_events.sh` and `03442_alter_delete_empty_part.sh`
# retry this pattern.
count_mutate_parts()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS part_log"
    $CLICKHOUSE_CLIENT -q "
        SELECT count()
        FROM system.part_log
        WHERE database = currentDatabase() AND table = 't_clear_coalesced_mutation'
          AND event_type = 'MutatePart'
          AND toUnixTimestamp64Micro(event_time_microseconds) > ${run_start}"
}

for _ in {1..120}
do
    [[ "$(count_mutate_parts)" -ge 1 ]] && break
    sleep 0.5
done

# The count alone cannot prove coalescing: a split pass commits each replacement part before
# enqueueing its own `MutatePart`, so the first event can be visible while the second is still on
# its way, and any read then sees exactly one row. `mutation_ids` settles it from a single row - a
# coalesced pass carries both mutation ids in one event, a split pass one id per event - so the
# assertion holds even when a second event has not arrived yet.
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS part_log"
$CLICKHOUSE_CLIENT -q "
    SELECT 'mutate_parts', count(), 'mutation_ids', max(length(mutation_ids))
    FROM system.part_log
    WHERE database = currentDatabase() AND table = 't_clear_coalesced_mutation'
      AND event_type = 'MutatePart'
      AND toUnixTimestamp64Micro(event_time_microseconds) > ${run_start}"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_clear_coalesced_mutation"
