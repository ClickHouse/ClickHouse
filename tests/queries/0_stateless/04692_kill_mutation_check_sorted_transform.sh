#!/usr/bin/env bash
# Tags: no-parallel, no-debug
# Test that KILL MUTATION interrupts a running mutation inside CheckSortedTransform.
# no-parallel: uses a PAUSEABLE_ONCE failpoint that fires exactly once globally.
# no-debug: background merges inject CheckSortedTransform only in debug builds, so in a debug
# build a concurrent merge could hit the failpoint instead of the mutation and make the test flaky.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_kill_mutation_check_sorted_transform_$RANDOM"
alter_stderr="${CLICKHOUSE_TMP}/${TABLE}.stderr"
alter_pid=""

cleanup()
{
    # Disable the failpoint first: if cleanup runs while the transform is paused at it, the mutation
    # would otherwise never resume and `wait "$alter_pid"` below would hang indefinitely.
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT check_sorted_transform_pause" 2>/dev/null ||:
    $CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "KILL MUTATION WHERE database = '${CLICKHOUSE_DATABASE}' AND table = '${TABLE}'" >/dev/null 2>&1 ||:
    [ -n "$alter_pid" ] && wait "$alter_pid" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.${TABLE} SYNC" 2>/dev/null ||:
    rm -f "$alter_stderr"
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.${TABLE}
    (
        id UInt64,
        value UInt64
    )
    ENGINE = MergeTree
    ORDER BY id
"
$CLICKHOUSE_CLIENT -q "INSERT INTO ${CLICKHOUSE_DATABASE}.${TABLE} SELECT number, number FROM numbers(100000)"

# A DELETE mutation affects all columns, so the reading pipeline contains the sorting key column
# and therefore CheckSortedTransform, which pauses at the failpoint before its row check loop.
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT check_sorted_transform_pause"

$CLICKHOUSE_CLIENT --query "
    ALTER TABLE ${CLICKHOUSE_DATABASE}.${TABLE} DELETE WHERE id > 50000 SETTINGS mutations_sync = 1
" >/dev/null 2>"$alter_stderr" &
alter_pid=$!

# Wait until the mutation pipeline has reached and paused inside CheckSortedTransform. The wait
# succeeds only if the failpoint was actually hit, i.e. the transform is blocking the mutation.
timeout 60 $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT check_sorted_transform_pause PAUSE"
if [ $? -ne 0 ]; then
    echo "FAIL: CheckSortedTransform was not reached within 60 s"
    exit 1
fi

# Cancel the running mutation. This must cancel the mutation pipeline so that CheckSortedTransform
# observes is_cancelled() and stops as soon as it resumes.
$CLICKHOUSE_CLIENT -q "KILL MUTATION WHERE database = '${CLICKHOUSE_DATABASE}' AND table = '${TABLE}'" >/dev/null 2>&1

# Release the paused transform thread.
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT check_sorted_transform_pause"

# The foreground ALTER (mutations_sync=1) must exit through the expected cancellation path once the
# mutation is killed. Wait only ~15 s (a small multiple of one chunk rewrite): the within-chunk cancel
# makes the pull return as soon as the paused transform resumes, so real cancellation latency is well
# under this bound. A regression that cancels only later, or fails to cancel at all, must fail the test.
for _ in $(seq 1 150); do
    kill -0 "$alter_pid" 2>/dev/null || break
    sleep 0.1
done
if kill -0 "$alter_pid" 2>/dev/null; then
    echo "FAIL: ALTER still running 15 s after KILL MUTATION (cancellation did not take effect)"
    exit 1
fi
wait "$alter_pid"
alter_rc=$?

# If the ALTER failed, it must have failed due to the cancellation, not some other error.
if [ "$alter_rc" -ne 0 ] && ! grep -qiE 'killed|ABORTED|UNFINISHED|Cancelled' "$alter_stderr"; then
    echo "FAIL: ALTER failed for a non-cancellation reason (rc=$alter_rc): $(cat "$alter_stderr")"
    exit 1
fi

# Prove the mutation was actually killed (no full rewrite committed) rather than merely having
# unchanged data by coincidence: no successfully-completed mutation may remain.
done_ok=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.mutations WHERE database = '${CLICKHOUSE_DATABASE}' AND table = '${TABLE}' AND is_done = 1 AND latest_fail_reason = ''")
if [ "${done_ok:-0}" != "0" ]; then
    echo "FAIL: a mutation completed successfully although it should have been killed"
    exit 1
fi

# The mutation was cancelled before writing anything, so the data must be unchanged.
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(id), sum(value) FROM ${CLICKHOUSE_DATABASE}.${TABLE}"
