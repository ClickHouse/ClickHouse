#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `logs_to_keep` above `UINT32_MAX` is rejected in a definition the user supplies now: a fresh
# `CREATE` and, equally, a full-syntax `ATTACH`, which carries a user-written definition. It is
# clamped only on the paths that replay a value an older server already accepted:
#   1. The metadata file, replayed on server startup and by the short-syntax `ATTACH DATABASE db`.
#      Rejecting there would stop a server that is healthy today from starting, because the old wrap
#      was invisible - `10000000000` became 1410065408 and behaved as the operator intended. Every
#      path that writes the file now validates the value, so a stateless test cannot fabricate such
#      a file; that clamp is covered by an integration test that edits the file directly.
#   2. The `/logs_to_keep` node, which the worker reads. The node and the metadata file are independent
#      stores, so a legacy value survives in Keeper even after the setting was narrowed. Covered here,
#      for both consumers of the value: the node is set out of range, and then the cleanup pass must
#      delete nothing and a replica that is merely behind must not be declared lost.

node_db="${CLICKHOUSE_DATABASE}_node"
node_path="/test/${CLICKHOUSE_DATABASE}/node"

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $node_db SYNC"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE $node_db ENGINE = Replicated('$node_path', 's1', 'r1') SETTINGS logs_to_keep = 1000"

# Each CREATE TABLE adds one DDL log entry. The entries are enqueued synchronously by the query, which
# is all the counts below depend on; the execution is left asynchronous because waiting for it is what
# makes replicated DDL slow.
$CLICKHOUSE_CLIENT --distributed_ddl_task_timeout=0 --distributed_ddl_output_mode=none -q "
    CREATE TABLE $node_db.t1 (x UInt32) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE $node_db.t2 (x UInt32) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE $node_db.t3 (x UInt32) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE $node_db.t4 (x UInt32) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE $node_db.t5 (x UInt32) ENGINE = MergeTree ORDER BY x;"

log_entries() {
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.zookeeper WHERE path = '$node_path/log'"
}

cleanups() {
    $CLICKHOUSE_CLIENT -q "
        SYSTEM FLUSH LOGS text_log;
        SELECT count() FROM system.text_log
        WHERE logger_name = 'DDLWorker($node_db)' AND message = 'Cleaning queue'"
}

lost_warnings() {
    $CLICKHOUSE_CLIENT -q "
        SYSTEM FLUSH LOGS text_log;
        SELECT count() FROM system.text_log
        WHERE logger_name = 'DDLWorker($node_db)' AND message LIKE 'Replica seems to be lost%'"
}

# `Finishing replica initialization` is written after the lost-or-not decision and after the recovery
# that decision may trigger, and a re-attach always takes the branch that writes it, because the
# worker is constructed anew and its `first_initialization` is true. So once this line is in
# `text_log`, a `Replica seems to be lost` of the same initialization is there too - it was queued
# earlier and the same flush carried both.
initializations() {
    $CLICKHOUSE_CLIENT -q "
        SYSTEM FLUSH LOGS text_log;
        SELECT count() FROM system.text_log
        WHERE logger_name = 'DDLWorker($node_db)' AND message LIKE 'Finishing replica initialization%'"
}

# `SYSTEM SYNC DATABASE REPLICA` cannot be the synchronization point for the cases below: an entry
# skipped as already processed does not move the log pointer, so once the pointer is rolled back the
# sync waits for a pointer that nothing advances.
wait_for_initialization() {
    for _ in $(seq 1 100); do
        if [ "$(initializations)" -gt "$1" ]; then
            return
        fi
        sleep 0.3
    done
    echo "timed out waiting for the replica to initialize"
}

echo -n "log entries: "
log_entries

# 2^32 + 4. `parse<UInt32>` used to wrap this to 4, which would leave only the last few entries and
# declare every replica that is further behind than that lost.
$CLICKHOUSE_KEEPER_CLIENT -q "set '$node_path/logs_to_keep' '4294967300'"

uuid=$($CLICKHOUSE_CLIENT -q "SELECT uuid FROM system.databases WHERE name = '$node_db'")
$CLICKHOUSE_CLIENT -q "DETACH DATABASE $node_db"
# The baseline is taken between DETACH and ATTACH: DETACH joins the old worker's threads, so any
# later `Cleaning queue` line can only come from the new worker, which holds the new value.
cleanups_before=$(cleanups)
# A full-syntax ATTACH is a definition the user supplies now, so an out-of-range value is rejected
# the same way CREATE rejects it, not clamped - it would have become the definition of record had
# the metadata file not existed.
echo -n "full-syntax attach with an out-of-range value: "
$CLICKHOUSE_CLIENT -q \
    "ATTACH DATABASE $node_db UUID '$uuid' ENGINE = Replicated('$node_path', 's1', 'r1') SETTINGS logs_to_keep = 10000000000" 2>&1 \
    | grep -o "BAD_ARGUMENTS" | head -1
# The rejection happened before anything was registered, so the database is still detached; the
# short syntax replays the metadata file, which holds the valid value from CREATE.
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE $node_db"
$CLICKHOUSE_CLIENT -q "SYSTEM SYNC DATABASE REPLICA $node_db"

# The expected outcome is "nothing deleted", which is indistinguishable from "cleanup did not run
# yet", so a cleanup pass of the new worker must be observed before counting. A freshly started
# worker runs one right away: the main thread sets `cleanup_event` before its first `scheduleTasks`,
# and the first pass is not gated by `cleanup_delay_period` - so this exits on the first check in
# practice, the loop bound is a failure cap only.
for _ in $(seq 1 50); do
    if [ "$(cleanups)" -gt "$cleanups_before" ]; then
        break
    fi
    sleep 0.3
done

echo -n "attached: "
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.databases WHERE name = '$node_db'"

echo -n "log entries after cleanup with an out-of-range value: "
log_entries

# The other consumer of the value: `initializeReplication` compares the same window against the lag
# of this replica to decide whether it is too far behind to catch up from the log and has to be
# rebuilt by `recoverLostReplica`. Out of range means "keep everything", so a replica that is behind
# must never be declared lost. The gap has to be wider than the window the overflowing check would
# wrap to - `parse<UInt32>` turns `4294967300` into 4 - hence ten more entries.
create_more_tables=""
for i in $(seq 6 15); do
    create_more_tables+="CREATE TABLE $node_db.t$i (x UInt32) ENGINE = MergeTree ORDER BY x;"
done
$CLICKHOUSE_CLIENT --distributed_ddl_task_timeout=0 --distributed_ddl_output_mode=none -q "$create_more_tables"
# The lag is the distance between the two pointers, so the replica has to start from a known one.
$CLICKHOUSE_CLIENT -q "SYSTEM SYNC DATABASE REPLICA $node_db"

replica_path="$node_path/replicas/s1|r1"

lag() {
    $CLICKHOUSE_CLIENT -q "
        SELECT (SELECT toUInt64(value) FROM system.zookeeper WHERE path = '$node_path' AND name = 'max_log_ptr')
             - (SELECT toUInt64(value) FROM system.zookeeper WHERE path = '$replica_path' AND name = 'log_ptr')"
}

$CLICKHOUSE_CLIENT -q "DETACH DATABASE $node_db"
initializations_before=$(initializations)
# The replica is put behind by rewriting its own pointer, because on a single server the log cannot
# advance while this replica is detached: enqueueing a DDL needs the database attached, and a second
# replica of the same database cannot execute one, as both would create the same table UUID under the
# same `store/` path. The entries themselves all stay in `/log` with their per-replica `finished`
# markers, so the ones the replica is now missing are replayed as already processed. 1 rather than 0,
# because a zero pointer means a brand new replica, which is recovered whatever the window is.
$CLICKHOUSE_KEEPER_CLIENT -q "set '$replica_path/log_ptr' '1'"

# The precondition of both cases below, asserted rather than counted from the entries: the lag has to
# exceed the window the overflowing check would wrap to, otherwise every build agrees that the
# replica is not lost and neither assertion says anything.
echo -n "lag exceeds the window the overflowing check would wrap to: "
[ "$(lag)" -gt 4 ] && echo 1 || echo 0

$CLICKHOUSE_CLIENT -q "ATTACH DATABASE $node_db"
wait_for_initialization "$initializations_before"

echo -n "lost replica warnings with an out-of-range value: "
lost_warnings

# The same two decisions with the value the overflowing check would wrap to, so that neither
# assertion above is vacuous: with a window of 4 a replica that far behind is lost, and the cleanup
# has to actually delete something. The pointer is rolled back again instead of relying on the case above having left
# it at 1, so this one does not depend on when exactly the pointer moves.
$CLICKHOUSE_KEEPER_CLIENT -q "set '$node_path/logs_to_keep' '4'"
$CLICKHOUSE_CLIENT -q "DETACH DATABASE $node_db"
initializations_before=$(initializations)
$CLICKHOUSE_KEEPER_CLIENT -q "set '$replica_path/log_ptr' '1'"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE $node_db"
wait_for_initialization "$initializations_before"

echo -n "lost replica warnings with 4: "
lost_warnings

# Here the deletion itself is the completion signal, so the log count is polled directly - no
# `text_log` round-trips.
for _ in $(seq 1 100); do
    if [ "$(log_entries)" -le 5 ]; then
        break
    fi
    sleep 0.3
done

echo -n "log entries after cleanup with 4: "
log_entries

$CLICKHOUSE_CLIENT -q "DROP DATABASE $node_db SYNC"
