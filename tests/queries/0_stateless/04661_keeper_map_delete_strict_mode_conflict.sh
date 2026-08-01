#!/usr/bin/env bash
# Tags: no-ordinary-database, zookeeper, no-fasttest, no-parallel
# no-parallel: uses a PAUSEABLE_ONCE failpoint, which fires exactly once globally; a concurrent
#   test could steal the pause and make this test hang.
#
# Regression test: with `keeper_map_strict_mode = 1`, a `DELETE` on a `KeeperMap` table must not fall
# back to unversioned per-key removals when the version-checked `multi` request fails. The fallback
# would delete rows based on a stale snapshot and still report the mutation as successful, breaking
# the documented guarantee that a strict-mode delete succeeds only if it is executed atomically.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

table="04661_keeper_map_delete_strict"
data_path="/test_keeper_map/$table/$CLICKHOUSE_DATABASE/data"

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT keeper_map_delete_pause_before_multi" 2>/dev/null ||:
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $table SYNC" 2>/dev/null ||:
}
trap cleanup EXIT
cleanup

$CLICKHOUSE_CLIENT -q "
CREATE TABLE $table (key UInt64, value UInt64)
ENGINE = KeeperMap('/$table/$CLICKHOUSE_DATABASE') PRIMARY KEY key"

# The key is base64-encoded into the node name, so insert the keys one at a time to learn which node
# belongs to which key instead of reimplementing the encoding here.
function setup_two_rows()
{
    $CLICKHOUSE_CLIENT -q "TRUNCATE TABLE $table"
    $CLICKHOUSE_CLIENT -q "INSERT INTO $table VALUES (1, 11)"
    node_for_key_1=$($CLICKHOUSE_CLIENT -q "SELECT name FROM system.zookeeper WHERE path = '$data_path'")
    $CLICKHOUSE_CLIENT -q "INSERT INTO $table VALUES (2, 22)"
}

# Both rows fit in a single block, so one `multi` request carries both version-checked removals.
function start_paused_delete()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT keeper_map_delete_pause_before_multi"
    $CLICKHOUSE_CLIENT --keeper_map_strict_mode 1 -q "ALTER TABLE $table DELETE WHERE 1" > /dev/null 2>&1 &
    delete_pid=$!
    $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT keeper_map_delete_pause_before_multi PAUSE"
}

function resume_and_report()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM NOTIFY FAILPOINT keeper_map_delete_pause_before_multi"
    wait $delete_pid && echo "mutation unexpectedly succeeded" || echo "mutation failed"
    $CLICKHOUSE_CLIENT -q "SELECT key, value FROM $table ORDER BY key"
}

echo "-- one of the matched keys is deleted concurrently"
# `multi` fails with `ZNONODE`. Previously the fallback removed the other matched key without its
# version check and the mutation reported success.
setup_two_rows
start_paused_delete
$CLICKHOUSE_KEEPER_CLIENT -q "rm '$data_path/$node_for_key_1'"
resume_and_report

echo "-- one matched key is deleted and another is updated concurrently"
# The block was read as `1@v0, 2@v0`; key `1` is removed and key `2` is updated, so its version is no
# longer `v0`. The unversioned fallback would delete the updated row from the stale snapshot.
setup_two_rows
start_paused_delete
$CLICKHOUSE_KEEPER_CLIENT -q "rm '$data_path/$node_for_key_1'"
$CLICKHOUSE_CLIENT -q "INSERT INTO $table VALUES (2, 222)"
resume_and_report
