#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: alter_pause_before_alter_lock / alter_pause_after_alter_under_lock are PAUSEABLE_ONCE and
#   fire globally for any ALTER on the server, so a concurrent test's ALTER could steal the one-shot pause
#   (same reasoning as 04057_backup_replicated_db_recreate.sh).

# Deterministic regression test for issue #110036 on a plain MergeTree table: a comment-only ALTER pins its
# metadata snapshot at access-check time (query-scoped metadata cache), before lockForAlter. If a concurrent
# ADD COLUMN commits in between (holding lockForAlter), the comment ALTER must not clobber it.
#   FP_PIN  = alter_pause_before_alter_lock:     comment ALTER pinned, parked before lockForAlter.
#   FP_HOLD = alter_pause_after_alter_under_lock: ADD COLUMN committed, still holding lockForAlter.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FP_PIN="alter_pause_before_alter_lock"
FP_HOLD="alter_pause_after_alter_under_lock"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP_PIN" > /dev/null 2>&1 ||:
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP_HOLD" > /dev/null 2>&1 ||:
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_alter_race"
$CLICKHOUSE_CLIENT --query "CREATE TABLE t_alter_race (n int, v UInt64) ENGINE = MergeTree ORDER BY n"

# Writer #2 (comment-only): pin the metadata snapshot at access check, park before lockForAlter.
# enable_shared_storage_snapshot_in_query MUST be 1 (the query-scoped cache is what gets pinned); pin it
# explicitly because clickhouse-test randomizes it (see clickhouse-test settings pool). With the cache off
# there is no pin and the race cannot occur (that is the negative-control case, covered separately).
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $FP_PIN"
$CLICKHOUSE_CLIENT --enable_shared_storage_snapshot_in_query=1 --query "ALTER TABLE t_alter_race COMMENT COLUMN v 'c0'" &
COMMENT_PID=$!
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FP_PIN PAUSE"

# Writer #1 (ADD COLUMN): FP_PIN is FIU_ONETIME and already consumed, so this ALTER passes it freely;
# it commits and parks with lockForAlter still held.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $FP_HOLD"
$CLICKHOUSE_CLIENT --query "ALTER TABLE t_alter_race ADD COLUMN m0 Int32" &
ADD_PID=$!
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FP_HOLD PAUSE"

# Resume writer #2 (it blocks on lockForAlter held by writer #1), then release writer #1. Mutex ordering
# makes the comment commit strictly follow the ADD COLUMN commit; the pin strictly precedes it.
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP_PIN"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP_HOLD"

wait $ADD_PID
wait $COMMENT_PID

# WP0-fixed: the column survives (1) and the comment is applied (c0).
# WP0-reverted: the comment ALTER commits the pinned pre-ADD snapshot in StorageMergeTree::alter
#   (isCommentAlter branch) and m0 is lost -> prints 0.
$CLICKHOUSE_CLIENT --query "SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = 't_alter_race' AND name = 'm0'"
$CLICKHOUSE_CLIENT --query "SELECT comment FROM system.columns WHERE database = currentDatabase() AND table = 't_alter_race' AND name = 'v'"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_alter_race"
