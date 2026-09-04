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
#      stores, so a legacy value survives in Keeper even after the setting was narrowed. Covered here:
#      the node is set out of range and the retention check must delete nothing.

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

# The same run with the value the old code wrapped to, so that the assertion above is not vacuous:
# this one has to actually delete something, and the deletion itself is the completion signal, so
# the log count is polled directly - no `text_log` round-trips.
$CLICKHOUSE_KEEPER_CLIENT -q "set '$node_path/logs_to_keep' '4'"
$CLICKHOUSE_CLIENT -q "DETACH DATABASE $node_db"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE $node_db"
for _ in $(seq 1 100); do
    if [ "$(log_entries)" -le 5 ]; then
        break
    fi
    sleep 0.3
done

echo -n "log entries after cleanup with 4: "
log_entries

$CLICKHOUSE_CLIENT -q "DROP DATABASE $node_db SYNC"
