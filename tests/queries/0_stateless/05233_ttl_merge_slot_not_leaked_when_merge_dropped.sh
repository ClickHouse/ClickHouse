#!/usr/bin/env bash
# Tags: no-parallel, no-shared-merge-tree
# Tag no-parallel: enables a global fail point, and the resource it is about is server-wide
# Tag no-shared-merge-tree: the fail point sits on the StorageMergeTree merge assignment path

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A merge with TTL takes one of the `max_number_of_merges_with_ttl_in_pool` slots the moment it is
# selected, which is long before the `system.merges` entry that accounts for it exists - the entry
# is created by the merge task, and only once that task starts running. A merge that is selected
# and then dropped before it ever runs therefore has to give the slot back on its own. It used to
# leak it instead, and since the slots are server-wide and there are two of them by default, two
# leaks stopped every table on the server from ever running a merge with TTL again.
#
# Dropping a selected merge is not exotic: the background pool discards a task that is still queued
# when its table goes away, which is what a `DROP TABLE` shortly after an insert of expired rows
# does. `mt_drop_selected_ttl_merge_once` reproduces exactly that, and only once - one leaked slot
# is enough to fail the probe below while leaving the other slot for the rest of the server.

function wait_until_empty()
{
    local table=$1
    local deadline=$((SECONDS + 120))
    while [ "$SECONDS" -lt "$deadline" ]; do
        if [ "$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM $table")" = "0" ]; then
            return
        fi
        sleep 0.3
    done
    echo "timed out waiting for the TTL merge on $table"
}

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_ttl_slot_dropped (d Date, x UInt64)
    ENGINE = MergeTree ORDER BY x
    TTL d + INTERVAL 1 DAY
    SETTINGS ttl_only_drop_parts = 1, merge_with_ttl_timeout = 0;

    -- Insert the expired part while merges are stopped, so that the fail point is already armed
    -- when the part becomes visible to the merge selector.
    SYSTEM STOP MERGES t_ttl_slot_dropped;
    INSERT INTO t_ttl_slot_dropped VALUES ('2000-01-01', 1);

    SYSTEM ENABLE FAILPOINT mt_drop_selected_ttl_merge_once;
    SYSTEM START MERGES t_ttl_slot_dropped;
"

# The first selected merge is dropped; the selector retries and the second one goes through, so the
# table emptying also tells us the fail point has been spent.
wait_until_empty "t_ttl_slot_dropped"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_ttl_slot_dropped;"

${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT mt_drop_selected_ttl_merge_once;"

# The probe insists on every slot being free: with one of them still held by the merge that was
# dropped above, this table can never run its own TTL merge.
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_ttl_slot_probe (d Date, x UInt64)
    ENGINE = MergeTree ORDER BY x
    TTL d + INTERVAL 1 DAY
    SETTINGS ttl_only_drop_parts = 1, merge_with_ttl_timeout = 0, max_number_of_merges_with_ttl_in_pool = 1;

    INSERT INTO t_ttl_slot_probe VALUES ('2000-01-01', 1);
"

wait_until_empty "t_ttl_slot_probe"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_ttl_slot_probe;"

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE t_ttl_slot_dropped;
    DROP TABLE t_ttl_slot_probe;
"
