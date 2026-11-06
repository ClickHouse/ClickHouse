#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings
# Tag no-parallel: creates and detaches a database
# Tag no-random-settings: relies on predictable per-row execution timing of `sleepEachRow`

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e -o pipefail

DB="db_04117_${CLICKHOUSE_DATABASE}"
SELECT_QID="select_04117_${CLICKHOUSE_DATABASE}"
DETACH_QID="detach_04117_${CLICKHOUSE_DATABASE}"

cleanup()
{
    # Kill the SELECT first so its `StoragePtr` reference is released. On a build that
    # does not include the cancellation check in `waitDetachedTableNotInUse`, killing
    # the DETACH first with SYNC would block until the SELECT finishes on its own.
    $CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '$SELECT_QID' SYNC FORMAT Null" 2>/dev/null || true
    $CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '$DETACH_QID' SYNC FORMAT Null" 2>/dev/null || true
    $CLICKHOUSE_CLIENT --database_atomic_wait_for_drop_and_detach_synchronously=0 --query "ATTACH DATABASE IF NOT EXISTS \`$DB\`" 2>/dev/null || true
    $CLICKHOUSE_CLIENT --database_atomic_wait_for_drop_and_detach_synchronously=0 --query "DROP DATABASE IF EXISTS \`$DB\` SYNC" 2>/dev/null || true
}
trap cleanup EXIT

wait_for_query_to_start()
{
    local qid="$1"
    local timeout=60
    local start=$EPOCHSECONDS
    while [[ "$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.processes WHERE query_id = '$qid'")" == "0" ]]; do
        if (( EPOCHSECONDS - start > timeout )); then
            echo "Timeout waiting for query $qid to start" >&2
            exit 1
        fi
        sleep 0.1
    done
}

$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS \`$DB\` SYNC"
$CLICKHOUSE_CLIENT --query "CREATE DATABASE \`$DB\` ENGINE = Atomic"
$CLICKHOUSE_CLIENT --query "CREATE TABLE \`$DB\`.t (x UInt64) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT --query "INSERT INTO \`$DB\`.t SELECT number FROM numbers(100)"

# Start a long-running SELECT that holds a `StoragePtr` reference to the table.
# - The WHERE clause references column `x` so the optimizer cannot eliminate the per-row
#   evaluation regardless of random settings or query condition caching.
# - With `max_threads=1` and 100 rows, `sleepEachRow(3)` is called sequentially per row,
#   yielding ~300 seconds of runtime. That is far longer than the test needs and gives
#   `waitDetachedTableNotInUse` a wide window to enter its busy-wait. The SELECT is
#   killed in `cleanup` so the long runtime never delays the test itself.
# - `function_sleep_max_microseconds_per_block` must be raised because the default 3 s
#   safety threshold would make `sleepEachRow(3) * 100 rows` throw `TOO_SLOW`.
$CLICKHOUSE_CLIENT --query_id "$SELECT_QID" \
    --max_threads=1 \
    --function_sleep_max_microseconds_per_block=600000000 \
    --query "SELECT count() FROM \`$DB\`.t WHERE x + sleepEachRow(3) >= 0" >/dev/null 2>&1 &

wait_for_query_to_start "$SELECT_QID"

# Trigger synchronous DETACH DATABASE: it will reach `waitDetachedTableNotInUse` and busy-wait
# because the SELECT above keeps an extra `shared_ptr` reference to the table.
$CLICKHOUSE_CLIENT --query_id "$DETACH_QID" \
    --database_atomic_wait_for_drop_and_detach_synchronously=1 \
    --query "DETACH DATABASE \`$DB\`" >/dev/null 2>&1 &
DETACH_BG_PID=$!

wait_for_query_to_start "$DETACH_QID"

# Cancel the synchronous DETACH. With the fix, the busy-wait observes the cancellation
# on its next iteration and the query exits promptly. Without the fix, the busy-wait
# loop ignores `KILL QUERY` and the DETACH would hang until the SELECT finishes.
# Use `ASYNC` so the kill returns immediately. With `SYNC`, the kill itself would block
# until the DETACH actually exits, masking the bug: on a buggy build the kill would
# wait the full 300 seconds for the SELECT to finish (and for the DETACH to then
# complete naturally), and the timing-based check below would still print
# "DETACH cancelled".
$CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '$DETACH_QID' ASYNC FORMAT Null" >/dev/null

# Allow up to a few seconds for the cancelled DETACH client to exit, but well under
# the SELECT's total sleep budget (100 rows * 3 seconds = 300 seconds).
detach_exited=0
for _ in $(seq 1 100); do
    if ! kill -0 "$DETACH_BG_PID" 2>/dev/null; then
        detach_exited=1
        break
    fi
    sleep 0.1
done

if (( detach_exited == 1 )); then
    echo "DETACH cancelled"
else
    echo "DETACH still running after KILL QUERY"
fi

# Kill the SELECT now to release the `StoragePtr` reference. On a buggy build, where
# `KILL QUERY` cannot interrupt the busy-wait in `waitDetachedTableNotInUse`, this is
# what allows the DETACH to actually finish, so that the `wait` below does not block
# for the full ~300 seconds of the SELECT's `sleepEachRow` budget.
$CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '$SELECT_QID' SYNC FORMAT Null" 2>/dev/null || true

wait "$DETACH_BG_PID" 2>/dev/null || true
