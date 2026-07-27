#!/usr/bin/env bash
# Tags: no-parallel

# Tests for SYSTEM WAIT FAILPOINT ... PAUSE:
#
# 1. Happy path: a real thread pauses at the failpoint; WAIT should return.
#    Uses infinite_sleep (triggered by SELECT sleep()), which has a real C++
#    call site at src/Functions/sleep.cpp.
#
# 2. Bug-fix path: DISABLE is called while WAIT is blocking and no thread has
#    paused. Before the fix, waitForPause hung indefinitely because the predicate
#    (pause_epoch > resume_epoch) could never become true after disable.
#
# 3. Re-enable cycle: verify that after disable+re-enable both paths still work,
#    i.e. the disabled flag is reset (a fresh FailPointChannel is created).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# --- Part 1: happy path ---
# SELECT sleep(0) pauses at the infinite_sleep failpoint before doing the actual
# sleep, so the query stays paused until DISABLE is called.
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT infinite_sleep"

$CLICKHOUSE_CLIENT -q "SELECT sleep(0) FORMAT Null" &
SLEEP_PID=$!

# Give the query time to reach and pause at the failpoint.
sleep 0.5

# WAIT should return immediately because the thread is already paused
# (pause_epoch > resume_epoch).
if timeout 5 $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT infinite_sleep PAUSE"; then
    echo "OK"
else
    echo "FAIL: WAIT FAILPOINT timed out on happy path"
fi

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT infinite_sleep"
wait "$SLEEP_PID"

# --- Parts 2 & 3: DISABLE while no thread is paused (the bug), and re-enable ---
# timeout exits with code 124 if the inner command does not finish in time.
wait_or_timeout()
{
    local fp="$1"
    timeout 5 $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT ${fp} PAUSE" &
    local pid=$!
    sleep 0.5
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT ${fp}"
    wait "$pid"
    local exit_code=$?
    if [ "$exit_code" -eq 0 ]; then
        echo "OK"
    elif [ "$exit_code" -eq 124 ]; then
        echo "FAIL: WAIT FAILPOINT timed out after 5s (bug present)"
    else
        echo "FAIL: WAIT FAILPOINT returned non-zero ($exit_code)"
    fi
}

# Part 2: first cycle.
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT dummy_pausable_failpoint"
wait_or_timeout dummy_pausable_failpoint

# Part 3: re-enable creates a fresh FailPointChannel (disabled=false).
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT dummy_pausable_failpoint"
wait_or_timeout dummy_pausable_failpoint
