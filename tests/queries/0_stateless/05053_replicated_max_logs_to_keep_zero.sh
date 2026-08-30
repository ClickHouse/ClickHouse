#!/usr/bin/env bash
# Tags: zookeeper, replica, no-shared-merge-tree, no-replicated-database
# no-shared-merge-tree: SharedMergeTree does not run ReplicatedMergeTreeCleanupThread, so /log is never trimmed
# no-replicated-database: the ZooKeeper path and the replica name are literal, so they collide between the hosts of the database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# min_replicated_logs_to_keep = 1 lets the cleanup thread reach the log threshold computation with a
# single /log child. cleanup_thread_preferred_points_per_iteration = 0 keeps its sleep at
# cleanup_delay_period instead of interpolating it up towards max_cleanup_delay_period.
# Stopping merges keeps the inserted parts in place, which bounds how much /log grows.
# The threshold entry's name is only ever compared against the log pointer of a replica that is
# not active, so an inactive replica is what makes the trim depend on which entry was chosen.
$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS max_logs_zero SYNC;
    DROP TABLE IF EXISTS max_logs_zero_2 SYNC;
    CREATE TABLE max_logs_zero (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/r', '1') ORDER BY x
    SETTINGS max_replicated_logs_to_keep = 0, min_replicated_logs_to_keep = 1,
             cleanup_delay_period = 0, cleanup_delay_period_random_add = 1,
             cleanup_thread_preferred_points_per_iteration = 0;
    CREATE TABLE max_logs_zero_2 (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/r', '2') ORDER BY x
    SETTINGS max_replicated_logs_to_keep = 0, min_replicated_logs_to_keep = 1,
             cleanup_delay_period = 0, cleanup_delay_period_random_add = 1,
             cleanup_thread_preferred_points_per_iteration = 0;
    DETACH TABLE max_logs_zero_2;
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
for _ in {1..120}; do
    count=$($CLICKHOUSE_CLIENT --query "SELECT numChildren FROM system.zookeeper WHERE path = '/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/r' AND name = 'log'" 2>"$poll_err")
    rc=$?
    [[ $rc -eq 0 ]] && [[ $count =~ ^[0-9]+$ ]] && [[ $count -eq 1 ]] && { trimmed=1; break; }
    sleep 1
done

echo "$trimmed"
if [[ $trimmed != 1 ]]; then
    echo "cleanup wait did not converge: last count='$count', last poll error:" >&2
    cat "$poll_err" >&2
fi
rm -f "$poll_err"

# At least 8 entries reached /log, so the single remaining child is the result of a trim rather than
# of a log that was never longer than that.
$CLICKHOUSE_CLIENT --query "SELECT log_pointer >= 8 FROM system.replicas WHERE database = currentDatabase() AND table = 'max_logs_zero'"

$CLICKHOUSE_CLIENT --query "SELECT value FROM system.zookeeper WHERE path = '/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/r/replicas/1' AND name = 'is_lost'"
$CLICKHOUSE_CLIENT --query "SELECT value FROM system.zookeeper WHERE path = '/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/r/replicas/2' AND name = 'is_lost'"

$CLICKHOUSE_CLIENT --query "ATTACH TABLE max_logs_zero_2"
$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS max_logs_zero SYNC;
    DROP TABLE IF EXISTS max_logs_zero_2 SYNC;
"
