#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-shared-merge-tree, no-replicated-database, no-fasttest
# Tag no-parallel: blocks on a PAUSEABLE_ONCE failpoint that fires once globally, so a concurrent copy
#                  can consume the pause this one waits for
# Tag no-shared-merge-tree: the drop-replace intent lives in ReplicatedMergeTreeQueue; SharedMergeTree
#                           uses a different queue
# Tag no-replicated-database: uses explicit ReplicatedMergeTree ZooKeeper paths

# REPLACE PARTITION installs a drop-replace intent before its DROP_RANGE entry exists, so a mutation
# entry scheduled in that window is refused by shouldExecuteLogEntry rather than allowed to produce a
# part inside the range being replaced. The refusal surfaces as replication_queue.postpone_reason,
# which is where shouldExecuteLogEntry writes its out-param.
#
# The intent window is entered deterministically: cancelRemovedPartsCheck, called by
# replacePartitionFrom while the intent is held, pauses on
# rmt_cancel_removed_parts_check_pause_in_gap.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FAILPOINT="rmt_cancel_removed_parts_check_pause_in_gap"

# A paused thread stays parked until the failpoint is notified or disabled, so an early exit below
# would leave the REPLACE parked and the table undroppable.
function cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT ${FAILPOINT}" 2>/dev/null ||:
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS dst SYNC" 2>/dev/null ||:
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS src SYNC" 2>/dev/null ||:
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE dst (p UInt8, x UInt64, y UInt64 DEFAULT 0)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/dst', 'r1')
    PARTITION BY p ORDER BY x
    SETTINGS number_of_free_entries_in_pool_to_execute_mutation = 0;
    CREATE TABLE src (p UInt8, x UInt64, y UInt64 DEFAULT 0)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/src', 'r1')
    PARTITION BY p ORDER BY x;
"

# y distinguishes the two tables' rows, so the final assertion shows which table partition 1 holds.
${CLICKHOUSE_CLIENT} -q "INSERT INTO dst SELECT 1, number, 0 FROM numbers(100) SETTINGS insert_keeper_fault_injection_probability = 0"
${CLICKHOUSE_CLIENT} -q "INSERT INTO src SELECT 1, number, 1000 FROM numbers(10) SETTINGS insert_keeper_fault_injection_probability = 0"

${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT ${FAILPOINT}"

REPLACE_OUT="${CLICKHOUSE_TMP}/04201_replace_partition_drop_intent.out"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE dst REPLACE PARTITION id '1' FROM src" >"$REPLACE_OUT" 2>&1 &
REPLACE_PID=$!

${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT ${FAILPOINT} PAUSE"

# The intent is installed and the REPLACE is parked, so this mutation's entry meets the guard.
${CLICKHOUSE_CLIENT} -q "ALTER TABLE dst UPDATE y = y + 1 WHERE p = 1"

# The guard runs before any entry-type branch in shouldExecuteLogEntry, so a fetch or merge entry
# producing a part in the replaced range writes the same reason. The claim here is about the
# mutation, so the entry type is named.
postponed=no
for _ in {1..120}; do
    if ${CLICKHOUSE_CLIENT} -q "
        SELECT postpone_reason FROM system.replication_queue
        WHERE database = currentDatabase() AND table = 'dst' AND type = 'MUTATE_PART' FORMAT TSVRaw" 2>/dev/null \
        | grep -qF "because there is a drop or replace intent with part name"; then
        postponed=yes
        break
    fi
    sleep 0.5
done
# The reason string embeds generated part names, so report the outcome rather than the raw text.
echo "mutation postponed by drop-replace intent: $postponed"

${CLICKHOUSE_CLIENT} -q "SYSTEM NOTIFY FAILPOINT ${FAILPOINT}"
wait $REPLACE_PID 2>/dev/null ||:
cat "$REPLACE_OUT"
rm -f "$REPLACE_OUT"

# The row assertion below is satisfied by the REPLACE alone, so a mutation that never settles must
# say so rather than fall through to it. Silent on success, so the reference is unchanged.
settled=no
for _ in {1..120}; do
    [ "$(${CLICKHOUSE_CLIENT} -q "
        SELECT count() FROM system.mutations
        WHERE database = currentDatabase() AND table = 'dst' AND is_done = 0")" = "0" ] && { settled=yes; break; }
    sleep 0.5
done
[ "$settled" = "yes" ] || echo "mutation did not settle"

# Partition 1 must be exactly src's 10 rows: no dst row survives, and none is resurrected by the
# postponed mutation.
echo "rows in partition 1, and how many carry src's y:"
${CLICKHOUSE_CLIENT} -q "SELECT count(), countIf(y = 1000) FROM dst WHERE p = 1"
