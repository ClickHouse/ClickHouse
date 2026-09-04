#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `logs_to_keep` above `UINT32_MAX` is rejected for a fresh `CREATE` (see 05060), but it is clamped
# on the paths that replay a value an older server already accepted:
#   1. `ATTACH`, which replays the metadata file. Rejecting there would stop a server that is healthy
#      today from starting, because the old wrap was invisible - `10000000000` became 1410065408 and
#      behaved as the operator intended.
#   2. The `/logs_to_keep` node, which the worker reads. The node and the metadata file are independent
#      stores, so a legacy value survives in Keeper even after the setting was narrowed.
# One database exercises both: the Keeper node is set out of range and the re-`ATTACH` carries an
# out-of-range `SETTINGS` clause, so a single restart covers the metadata clamp (the warning), the
# node clamp, and the retention check with the clamped value (nothing deleted).

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

echo -n "log entries: "
log_entries

# 2^32 + 4. `parse<UInt32>` used to wrap this to 4, which would leave only the last few entries and
# declare every replica that is further behind than that lost.
$CLICKHOUSE_KEEPER_CLIENT -q "set '$node_path/logs_to_keep' '4294967300'"

# Unique per invocation, so that rows left in `text_log` by an earlier run of this test are not counted.
attach_query_id="05061_attach_${CLICKHOUSE_DATABASE}_$(cat /proc/sys/kernel/random/uuid)"

uuid=$($CLICKHOUSE_CLIENT -q "SELECT uuid FROM system.databases WHERE name = '$node_db'")
$CLICKHOUSE_CLIENT -q "DETACH DATABASE $node_db"
# The baseline is taken between DETACH and ATTACH: DETACH joins the old worker's threads, so any
# later `Cleaning queue` line can only come from the new worker, which holds the new value.
cleanups_before=$(cleanups)
# The clamp warning is asserted through `system.text_log` below, so the copy the client prints on
# stderr is dropped here - the harness runs with server logs at warning level.
$CLICKHOUSE_CLIENT --query_id "$attach_query_id" -q \
    "ATTACH DATABASE $node_db UUID '$uuid' ENGINE = Replicated('$node_path', 's1', 'r1') SETTINGS logs_to_keep = 10000000000" 2>/dev/null
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

# No extra flush: `cleanups` above flushed `text_log` after the ATTACH had logged the warning.
echo -n "clamp warning: "
$CLICKHOUSE_CLIENT -q "
    SELECT count() FROM system.text_log
    WHERE query_id = '$attach_query_id'
      AND logger_name = 'DatabaseReplicatedSettings'
      AND message LIKE '%exceeds the maximum of 4294967295%'"

echo -n "log entries after cleanup with an out-of-range value: "
log_entries

# The same run with the value the old code wrapped to, so that the assertion above is not vacuous:
# this one has to actually delete something, and the deletion itself is the completion signal, so
# the log count is polled directly - no `text_log` round-trips. The plain ATTACH replays the
# metadata file, which now holds the out-of-range value from the explicit ATTACH above, so this
# also covers the clamp on the metadata replay path (the warning goes to stderr and is dropped).
$CLICKHOUSE_KEEPER_CLIENT -q "set '$node_path/logs_to_keep' '4'"
$CLICKHOUSE_CLIENT -q "DETACH DATABASE $node_db"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE $node_db" 2>/dev/null
for _ in $(seq 1 100); do
    if [ "$(log_entries)" -le 5 ]; then
        break
    fi
    sleep 0.3
done

echo -n "log entries after cleanup with 4: "
log_entries

$CLICKHOUSE_CLIENT -q "DROP DATABASE $node_db SYNC"
