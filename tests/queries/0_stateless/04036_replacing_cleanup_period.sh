#!/usr/bin/env bash
# Tags: long
#
# Tests for replacing_merge_cleanup_period_seconds setting.
# Verifies that the background merge thread automatically triggers a FINAL CLEANUP
# merge after the configured period, physically removing is_deleted=1 rows and
# deduplicating updates without requiring an explicit OPTIMIZE command.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Wait for deleted rows to be physically absent from $1 (timeout in seconds).
# Returns 0 if cleanup succeeded, 1 if timed out.
wait_for_cleanup()
{
    local table=$1
    local timeout=$2
    local res
    for _ in $(seq "$timeout")
    do
        sleep 1
        res=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM $table WHERE deleted = 1")
        if [ "$res" -eq 0 ]
        then
            return 0
        fi
    done
    return 1
}

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t04036_cleanup_period"

# Create table with a 1-second cleanup period so the background thread fires quickly.
# merge_selecting_sleep_ms=500 makes the background scheduler wake up every 0.5s.
$CLICKHOUSE_CLIENT -q "
CREATE TABLE t04036_cleanup_period
(
    id      UInt64,
    ver     UInt64,
    deleted UInt8
)
ENGINE = ReplacingMergeTree(ver, deleted)
ORDER BY id
SETTINGS
    allow_experimental_replacing_merge_with_cleanup = 1,
    replacing_merge_cleanup_period_seconds = 1,
    merge_selecting_sleep_ms = 500"

# Insert multiple versions of the same key including a deletion.
# Each INSERT creates a separate part (5 parts total).
$CLICKHOUSE_CLIENT -q "INSERT INTO t04036_cleanup_period VALUES (1, 1, 0)" # insert key=1 ver=1
$CLICKHOUSE_CLIENT -q "INSERT INTO t04036_cleanup_period VALUES (1, 2, 0)" # update key=1 to ver=2
$CLICKHOUSE_CLIENT -q "INSERT INTO t04036_cleanup_period VALUES (2, 1, 0)" # insert key=2 ver=1
$CLICKHOUSE_CLIENT -q "INSERT INTO t04036_cleanup_period VALUES (2, 2, 1)" # delete key=2 at ver=2
$CLICKHOUSE_CLIENT -q "INSERT INTO t04036_cleanup_period VALUES (3, 1, 0)" # insert key=3 (never updated)

# Wait for the periodic cleanup merge to physically remove the deleted row.
# The timer fires after 1s regardless of whether a regular merge ran first, because
# the cleanup check allows single-part rewrites to remove is_deleted=1 rows.
if wait_for_cleanup 't04036_cleanup_period' 60
then
    echo "Cleanup OK"
else
    echo "Cleanup FAILED (timeout)"
fi

# After cleanup: key=2 is physically gone, key=1 is at ver=2, key=3 is at ver=1.
$CLICKHOUSE_CLIENT -q "SELECT id, ver, deleted FROM t04036_cleanup_period ORDER BY id"

# Insert more versions to verify that updates are also deduplicated by the timer.
$CLICKHOUSE_CLIENT -q "INSERT INTO t04036_cleanup_period VALUES (1, 3, 0)" # update key=1 to ver=3
$CLICKHOUSE_CLIENT -q "INSERT INTO t04036_cleanup_period VALUES (1, 4, 0)" # update key=1 to ver=4

if wait_for_cleanup 't04036_cleanup_period' 60
then
    echo "Second cleanup OK"
else
    echo "Second cleanup FAILED (timeout)"
fi

# Only ver=4 of key=1 should survive alongside key=3.
$CLICKHOUSE_CLIENT -q "SELECT id, ver, deleted FROM t04036_cleanup_period ORDER BY id"

$CLICKHOUSE_CLIENT -q "DROP TABLE t04036_cleanup_period"
