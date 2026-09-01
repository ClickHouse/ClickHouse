#!/usr/bin/env bash
# Tags: zookeeper, replica, no-shared-merge-tree, no-replicated-database
# no-shared-merge-tree: SharedMergeTree does not run ReplicatedMergeTreeCleanupThread, so /log is never trimmed
# no-replicated-database: the ZooKeeper path and the replica name are literal, so they collide between the hosts of the database
# Random settings limits: index_granularity=(8192, 8192); index_granularity_bytes=(10485760, 10485760)
# Both are part of the table metadata stored in ZooKeeper, and the ATTACH below has to agree with
# what the CREATE stored there, so this test pins them on both statements instead of randomizing.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

ZK_PATH="/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX"

# The oldest kept log record names the threshold that log pointers of inactive replicas are
# compared with, so keeping none of them is not a valid configuration.
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE max_logs_reject (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/reject', '1') ORDER BY x
    SETTINGS max_replicated_logs_to_keep = 0;
" 2>&1 | grep -F -q "max_replicated_logs_to_keep: value 0 makes no sense" && echo 1 || echo 0

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE max_logs_reject (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/reject', '1') ORDER BY x;
"
$CLICKHOUSE_CLIENT --query "
    ALTER TABLE max_logs_reject MODIFY SETTING max_replicated_logs_to_keep = 0;
" 2>&1 | grep -F -q "max_replicated_logs_to_keep: value 0 makes no sense" && echo 1 || echo 0

# Table settings are checked when a table is created or altered, not when a table is loaded from
# metadata that already exists, so the cleanup thread still has to cope with 0. An `ATTACH` with a
# full definition reaches that state from SQL, over the same load path a table created by an older
# server takes, and leaves a writable replica.
#
# min_replicated_logs_to_keep = 1 lets the cleanup thread reach the log threshold computation with a
# single /log child. cleanup_thread_preferred_points_per_iteration = 0 keeps its sleep at
# cleanup_delay_period instead of interpolating it up towards max_cleanup_delay_period.
# Stopping merges keeps the inserted parts in place, which bounds how much /log grows.
# The threshold entry's name is only ever compared against the log pointer of a replica that is
# not active, so an inactive replica is what makes the trim depend on which entry was chosen.
CLEANUP_SETTINGS="min_replicated_logs_to_keep = 1, cleanup_delay_period = 0, cleanup_delay_period_random_add = 1, cleanup_thread_preferred_points_per_iteration = 0, index_granularity = 8192, index_granularity_bytes = 10485760"

# Replica 1 is registered by a plain CREATE, then detached so that the replica it registered can be
# picked up by the ATTACH below. Replica 2 stays detached for the rest of the test.
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE max_logs_registrar (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/r', '1') ORDER BY x
    SETTINGS max_replicated_logs_to_keep = 1, $CLEANUP_SETTINGS;
    CREATE TABLE max_logs_zero_2 (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/r', '2') ORDER BY x
    SETTINGS max_replicated_logs_to_keep = 1, $CLEANUP_SETTINGS;
    DETACH TABLE max_logs_zero_2;
    DETACH TABLE max_logs_registrar;
"

# A generated UUID, because a detached table keeps its own UUID mapping until it is dropped. The
# replica is identified by its ZooKeeper path and name, not by the UUID.
uuid=$($CLICKHOUSE_CLIENT --query "SELECT generateUUIDv4()")
# `send_logs_level=fatal` suppresses the "full table definition is not recommended" warning.
$CLICKHOUSE_CLIENT --send_logs_level fatal --query "
    ATTACH TABLE max_logs_zero UUID '$uuid' (x UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/r', '1') ORDER BY x
    SETTINGS max_replicated_logs_to_keep = 0, $CLEANUP_SETTINGS;
    SYSTEM STOP MERGES max_logs_zero;
"

# One /log entry per committed part, so these put at least 8 entries in /log.
$CLICKHOUSE_CLIENT --insert_keeper_fault_injection_probability=0 --async_insert=0 --query "
    INSERT INTO max_logs_zero VALUES (1);
    INSERT INTO max_logs_zero VALUES (2);
    INSERT INTO max_logs_zero VALUES (3);
    INSERT INTO max_logs_zero VALUES (4);
    INSERT INTO max_logs_zero VALUES (5);
    INSERT INTO max_logs_zero VALUES (6);
    INSERT INTO max_logs_zero VALUES (7);
    INSERT INTO max_logs_zero VALUES (8);
"

# The cleanup thread computes the log threshold before it trims /log, so a trim down to
# min_replicated_logs_to_keep is observable only if that computation completed. Only a read that
# returned a number ends the wait, so a client that dies mid-read cannot end it.
poll_err="${CLICKHOUSE_TMP}/05053_poll_err_${CLICKHOUSE_TEST_UNIQUE_NAME}"
trimmed=0
pulled=0
# Both conditions are polled and latched: log_pointer >= 8 is what distinguishes "one child after
# a trim" from "a log that never grew", a trim can become visible before the queue updater has
# pulled the last entry, and a merge entry can be logged after a trim.
for _ in {1..120}; do
    count=$($CLICKHOUSE_CLIENT --query "SELECT numChildren FROM system.zookeeper WHERE path = '$ZK_PATH/r' AND name = 'log'" 2>"$poll_err")
    rc=$?
    [[ $rc -eq 0 ]] && [[ $count =~ ^[0-9]+$ ]] && [[ $count -eq 1 ]] && trimmed=1
    pointer=$($CLICKHOUSE_CLIENT --query "SELECT log_pointer FROM system.replicas WHERE database = currentDatabase() AND table = 'max_logs_zero'" 2>>"$poll_err")
    rc=$?
    [[ $rc -eq 0 ]] && [[ $pointer =~ ^[0-9]+$ ]] && [[ $pointer -ge 8 ]] && pulled=1
    [[ $trimmed -eq 1 ]] && [[ $pulled -eq 1 ]] && break
    sleep 1
done

echo "$trimmed"
echo "$pulled"
if [[ $trimmed != 1 ]] || [[ $pulled != 1 ]]; then
    echo "cleanup wait did not converge: last count='$count', last log_pointer='$pointer', last poll error:" >&2
    cat "$poll_err" >&2
fi
rm -f "$poll_err"

$CLICKHOUSE_CLIENT --query "SELECT value FROM system.zookeeper WHERE path = '$ZK_PATH/r/replicas/1' AND name = 'is_lost'"
$CLICKHOUSE_CLIENT --query "SELECT value FROM system.zookeeper WHERE path = '$ZK_PATH/r/replicas/2' AND name = 'is_lost'"

# Dropping both attached tables removes every replica, and with it the whole ZooKeeper path. The
# detached registrar holds no replica of its own by then and goes away with the test database.
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS max_logs_zero SYNC"
$CLICKHOUSE_CLIENT --query "ATTACH TABLE max_logs_zero_2"
$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS max_logs_zero_2 SYNC;
    DROP TABLE IF EXISTS max_logs_reject SYNC;
"
