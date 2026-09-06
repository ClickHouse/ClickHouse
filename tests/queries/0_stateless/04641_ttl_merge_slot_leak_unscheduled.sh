#!/usr/bin/env bash
# Tags: long, no-parallel, no-shared-merge-tree, no-random-settings, no-random-merge-tree-settings
# ^ long: waits for the background merge pool to make progress within a bounded time window.
# ^ no-parallel: uses the global `mt_skip_scheduling_merge_once` failpoint (same as 04492).
# ^ no-shared-merge-tree: the `Manual` merge selector and the failpoint apply to plain `MergeTree`.
# ^ no-random-settings, no-random-merge-tree-settings: the test pins the settings that drive
#   the background TTL selection (cap, selector, `ttl_only_drop_parts`).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A TTL merge books a slot against `max_number_of_merges_with_ttl_in_pool` at SELECTION time; the
# booking is handed to the `MergeList` entry only when the merge starts executing. If the selected
# merge is dropped before that (here: the `mt_skip_scheduling_merge_once` failpoint, standing in for
# the pending-queue kill a `DROP`/`DETACH TABLE` performs), the slot must still be released. Otherwise
# the global counter leaks and, once it reaches the cap, background TTL merges stop server-wide until
# restart. cap=1 makes a single leak decisive. A second cycle on a fresh table, run after `DROP`ping
# the first so its whole task (and the slot accounting it carries) is destroyed, catches the opposite
# error: an over-released slot wraps the counter, so no further TTL merge is ever admitted.

${CLICKHOUSE_CLIENT} -m -q "
DROP TABLE IF EXISTS t_ttl_slot_leak;
DROP TABLE IF EXISTS t_ttl_slot_leak_2;

CREATE TABLE t_ttl_slot_leak (d Date, x UInt64)
ENGINE = MergeTree ORDER BY x
TTL d + INTERVAL 1 MONTH
SETTINGS ttl_only_drop_parts = 1, merge_with_ttl_timeout = 0,
         max_number_of_merges_with_ttl_in_pool = 1,
         merge_selector_algorithm = 'Manual';

SYSTEM STOP MERGES t_ttl_slot_leak;

-- One fully expired part and one fresh part. The TTL margin is much larger than one day so a
-- randomized \`session_timezone\` cannot shift \`today\` across the expiry boundary.
INSERT INTO t_ttl_slot_leak VALUES (today() - 100, 1);
INSERT INTO t_ttl_slot_leak VALUES (today(), 2);
"

# Both parts exist before the failpoint drops the first selected TTL merge (merges are still stopped).
echo "count after inserts: $(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_ttl_slot_leak")"

# Wait until the expired part is dropped by a background TTL merge. On a leaking build the slot is
# stuck at the cap (1), `merge_with_ttl_allowed` is false forever, and the expired part is never
# dropped; `count()` stays 2 until the deadline. On a fixed build the freed slot lets the next
# selection round re-pick the TTL-drop, and `count()` becomes 1 within seconds.
wait_for_count_1() {
    local table=$1
    for _ in $(seq 1 120); do
        if [[ "$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM $table")" -eq 1 ]]; then
            return 0
        fi
        # Nudge the background assignee so it re-evaluates promptly instead of sleeping on its backoff.
        ${CLICKHOUSE_CLIENT} -q "SYSTEM START MERGES $table"
        sleep 0.5
    done
    return 1
}

# Drop the first scheduled background merge exactly once. The TTL-drop is selected before any
# regular merge (and the `Manual` selector has no manual queue entry), so the failpoint consumes
# the TTL merge: its slot is booked, then the merge is dropped without ever executing.
${CLICKHOUSE_CLIENT} -m -q "
SYSTEM ENABLE FAILPOINT mt_skip_scheduling_merge_once;
SYSTEM START MERGES t_ttl_slot_leak;
"

if wait_for_count_1 t_ttl_slot_leak; then
    echo "OK: background TTL merge ran after a dropped selection"
else
    echo "FAIL: background TTL merge did not run (slot leaked)"
fi

# The `ONCE` failpoint auto-disables when it fires; still enabled here means cycle 1 never
# exercised the dropped-selection path. (Empty `system.fail_points` = failpoints compiled out.)
if [[ "$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.fail_points WHERE name = 'mt_skip_scheduling_merge_once' AND enabled")" -ne 0 ]]; then
    echo "FAIL: failpoint was not consumed by the first cycle"
fi

# Barrier: `DROP TABLE` waits (`removeTasksCorrespondingToStorage`) for cycle 1's task to be fully
# destroyed, including the entry that carries the slot accounting, so the counter has settled in
# every build type before cycle 2 begins.
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_ttl_slot_leak"

# Second cycle on a fresh table, with the `ONCE` failpoint already consumed: this merge must be
# selected, booked, executed, and destroyed with correct accounting against the GLOBAL counter.
# If cycle 1 over-released the slot, the counter wrapped when its task was destroyed and no TTL
# merge is ever admitted again (the cap can never exceed the wrapped value).
${CLICKHOUSE_CLIENT} -m -q "
CREATE TABLE t_ttl_slot_leak_2 (d Date, x UInt64)
ENGINE = MergeTree ORDER BY x
TTL d + INTERVAL 1 MONTH
SETTINGS ttl_only_drop_parts = 1, merge_with_ttl_timeout = 0,
         max_number_of_merges_with_ttl_in_pool = 1,
         merge_selector_algorithm = 'Manual';

SYSTEM STOP MERGES t_ttl_slot_leak_2;

INSERT INTO t_ttl_slot_leak_2 VALUES (today() - 100, 1);
INSERT INTO t_ttl_slot_leak_2 VALUES (today(), 2);

SYSTEM START MERGES t_ttl_slot_leak_2;
"

if wait_for_count_1 t_ttl_slot_leak_2; then
    echo "OK: second background TTL merge cycle ran"
else
    echo "FAIL: second TTL merge cycle did not run (slot over-released or leaked)"
fi

${CLICKHOUSE_CLIENT} -m -q "
SYSTEM DISABLE FAILPOINT mt_skip_scheduling_merge_once;
DROP TABLE IF EXISTS t_ttl_slot_leak_2;
"
