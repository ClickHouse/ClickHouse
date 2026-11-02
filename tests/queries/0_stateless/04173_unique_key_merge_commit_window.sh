#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-ordinary-database, no-replicated-database, no-shared-merge-tree
# UNIQUE KEY: what lands between a merge's snapshot and its commit, parked there by failpoints.
#   1. self-kill: a late kill re-expressed on the merge result does not pin that result
#   2. right row: a late kill lands on the right merged row, sorted and unsorted
# no-parallel: `unique_key_merge_pause_before_commit` is server-wide.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./parts.lib
. "$CURDIR"/parts.lib

set -e

CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --enable_unique_key 1"

cleanup() {
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT unique_key_merge_pause_before_commit" 2>/dev/null || true
}
trap cleanup EXIT

# `$1`'s active parts that hold rows, oldest first, as the list SYSTEM SCHEDULE MERGE takes.
data_parts() {
    $CLICKHOUSE_CLIENT --query "
        SELECT arrayStringConcat(groupArray(concat('''', name, '''')), ', ') FROM (
            SELECT name FROM system.parts
            WHERE database = currentDatabase() AND table = '$1' AND active AND rows > 0 ORDER BY min_block_number)
        FORMAT TSVRaw"
}

# The name of `uk_late`'s one active part matching `$1`.
uk_late_part() {
    $CLICKHOUSE_CLIENT --query "
        SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 'uk_late' AND active AND $1"
}

# Prints 1 once `uk_late` has no empty active part and no inactive part left and part `$1` is gone, else 0.
parts_reclaimed() {
    if wait_for_delete_empty_parts uk_late "$CLICKHOUSE_DATABASE" 120 \
        && wait_for_delete_inactive_parts uk_late "$CLICKHOUSE_DATABASE" 120 \
        && [[ $($CLICKHOUSE_CLIENT --query "
            SELECT count() FROM system.parts
            WHERE database = currentDatabase() AND table = 'uk_late' AND name = '$1'") == 0 ]]; then
        echo 1
    else
        echo 0
    fi
}

# 1. self-kill: red if a part's bitmap for itself pins it against removal
# (`self_killing_part_reclaimed` 0).
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS uk_late SYNC"

$CLICKHOUSE_CLIENT --query "
CREATE TABLE uk_late (id UInt64, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id)
SETTINGS merge_selector_algorithm = 'Manual',
         min_bytes_for_wide_part = 0,
         old_parts_lifetime = 0,
         cleanup_delay_period = 1,
         max_cleanup_delay_period = 1,
         cleanup_delay_period_random_add = 0"

$CLICKHOUSE_CLIENT --query "INSERT INTO uk_late SELECT number, 'a' FROM numbers(0, 10)"
$CLICKHOUSE_CLIENT --query "INSERT INTO uk_late SELECT number, 'b' FROM numbers(10, 10)"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT unique_key_merge_pause_before_commit"
$CLICKHOUSE_CLIENT --query "SYSTEM SCHEDULE MERGE uk_late PARTS $(data_parts uk_late)"

# Wait for the merge thread to reach the pause, not for it to show in `system.merges`.
$CLICKHOUSE_CLIENT --max_execution_time 120 --query "
    SYSTEM WAIT FAILPOINT unique_key_merge_pause_before_commit PAUSE"

# Both sources are still active, so this commits after the merge's snapshot.
$CLICKHOUSE_CLIENT --query "DELETE FROM uk_late WHERE id < 3"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT unique_key_merge_pause_before_commit"
$CLICKHOUSE_CLIENT --max_execution_time 120 --query "SYSTEM SYNC MERGES uk_late"

result=$(uk_late_part "rows > 0")
marker=$(uk_late_part "rows = 0")

$CLICKHOUSE_CLIENT --query "
    SELECT 'late_kill_on_the_result', arrayMap(x -> replaceAll(x, name, 'self'), unique_key_bitmap_versions)
    FROM system.parts WHERE database = currentDatabase() AND table = 'uk_late' AND name = '$result'"

# The marker's kills target the absorbed first part, so it stops pinning on its own.
echo -e "marker_for_an_absorbed_target_gone\t$(parts_reclaimed "$marker")"

$CLICKHOUSE_CLIENT --query "INSERT INTO uk_late SELECT number, 'c' FROM numbers(200, 5)"
$CLICKHOUSE_CLIENT --max_execution_time 120 --query "
    SYSTEM SCHEDULE MERGE uk_late PARTS $(data_parts uk_late)"
$CLICKHOUSE_CLIENT --max_execution_time 120 --query "SYSTEM SYNC MERGES uk_late"

echo -e "self_killing_part_reclaimed\t$(parts_reclaimed "$result")"

$CLICKHOUSE_CLIENT --query "
    SELECT 'survivors', groupArray(id) FROM (SELECT id FROM uk_late ORDER BY id)"

$CLICKHOUSE_CLIENT --query "DROP TABLE uk_late SYNC"

# 2. right row: red if a late kill ignores where the merge sorted its row, or the offset of its
# source part in an unsorted merge (`interleaved` / `no_sorting_key`).
# Not sparse: a merged sparse `id` reads its 0 last even under ORDER BY, a stock bug.
late_kill_lands_on_its_row() {
    local case=$1 order_by=$2 first=$3 second=$4 dead_at_snapshot=$5 late=$6
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS uk_late_map SYNC"
    $CLICKHOUSE_CLIENT --query "
        CREATE TABLE uk_late_map (id UInt64, v String)
        ENGINE = MergeTree UNIQUE KEY (id) ORDER BY $order_by
        SETTINGS merge_selector_algorithm = 'Manual', min_bytes_for_wide_part = 0,
                 ratio_of_defaults_for_sparse_serialization = 1"
    $CLICKHOUSE_CLIENT --query "INSERT INTO uk_late_map SELECT id, 'a' FROM ($first)"
    $CLICKHOUSE_CLIENT --query "INSERT INTO uk_late_map SELECT id, 'b' FROM ($second)"
    $CLICKHOUSE_CLIENT --query "DELETE FROM uk_late_map WHERE id IN $dead_at_snapshot"

    $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT unique_key_merge_pause_before_commit"
    $CLICKHOUSE_CLIENT --query "SYSTEM SCHEDULE MERGE uk_late_map PARTS $(data_parts uk_late_map)"
    $CLICKHOUSE_CLIENT --max_execution_time 120 --query "
        SYSTEM WAIT FAILPOINT unique_key_merge_pause_before_commit PAUSE"
    $CLICKHOUSE_CLIENT --query "DELETE FROM uk_late_map WHERE id IN $late"
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT unique_key_merge_pause_before_commit"
    $CLICKHOUSE_CLIENT --max_execution_time 120 --query "SYSTEM SYNC MERGES uk_late_map"

    $CLICKHOUSE_CLIENT --query "
        SELECT '$case', groupArray(id) FROM (SELECT id FROM uk_late_map ORDER BY id)"
    $CLICKHOUSE_CLIENT --query "DROP TABLE uk_late_map SYNC"
}

EVENS="SELECT number * 2 AS id FROM numbers(10)"
ODDS="SELECT number * 2 + 1 AS id FROM numbers(10)"

# Evens in one part, odds in the other; every late kill follows a snapshot-dead row.
late_kill_lands_on_its_row interleaved "(id)" "$EVENS" "$ODDS" "(2, 5)" "(8, 9, 17)"

late_kill_lands_on_its_row no_sorting_key "tuple()" \
    "SELECT number AS id FROM numbers(10)" "SELECT number + 10 AS id FROM numbers(10)" "(1, 12)" "(5, 15)"
