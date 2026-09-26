#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: `SYSTEM DISABLE ALL FAILPOINTS` disarms every fail point on the server.

# The contract of `SYSTEM DISABLE ALL FAILPOINTS` is not only that `enabled` drops to 0 in
# `system.fail_points` (that is pinned by 05223_system_disable_all_fault_injection_points), but
# that threads blocked on pauseable fail points are released, the way `SYSTEM DISABLE FAILPOINT`
# releases them one at a time - see 04095_wait_fp_pause_disable.
#
# 1. A real thread is parked on `infinite_sleep` (hit by `SELECT sleep(0)`, see
#    src/Functions/sleep.cpp); after `DISABLE ALL` the query must finish.
# 2. A `SYSTEM WAIT FAILPOINT ... PAUSE` blocked on a pauseable fail point nobody hits must
#    return too - that is the `disabled` flag of the wait channel, which `DISABLE ALL` has to
#    set on every channel it drops.
# 3. Re-enabling afterwards gets a fresh channel: the pause/wait cycle still works.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Reports how a background client finished: `timeout` exits with 124 when the inner command
# is still running after the deadline, which is exactly the hang this test guards against.
report_exit()
{
    local what="$1"
    local pid="$2"
    wait "$pid"
    local exit_code=$?
    if [ "$exit_code" -eq 0 ]; then
        echo "OK: $what"
    elif [ "$exit_code" -eq 124 ]; then
        echo "FAIL: $what still blocked after the deadline"
    else
        echo "FAIL: $what returned non-zero ($exit_code)"
    fi
}

# --- Part 1: a thread parked on a pauseable fail point is released ---
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT infinite_sleep"

timeout 120 $CLICKHOUSE_CLIENT -q "SELECT sleep(0) FORMAT Null" &
SLEEP_PID=$!

# Deterministic: returns once the thread has actually paused at the fail point.
timeout 120 $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT infinite_sleep PAUSE" && echo "OK: thread paused on infinite_sleep"

# --- Part 2: a WAIT ... PAUSE waiter with nobody paused is released as well ---
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT dummy_pausable_failpoint"

timeout 120 $CLICKHOUSE_CLIENT --query_id "wait_${CLICKHOUSE_TEST_UNIQUE_NAME}" -q "SYSTEM WAIT FAILPOINT dummy_pausable_failpoint PAUSE" &
WAIT_PID=$!

# Let the waiter reach the server before disabling. If it arrives late anyway the channel is
# already gone and `WAIT` returns at once, so the assertion below cannot hang either way.
for _ in $(seq 1 100)
do
    if [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = 'wait_${CLICKHOUSE_TEST_UNIQUE_NAME}'")" = "1" ]
    then
        break
    fi
    sleep 0.1
done

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE ALL FAILPOINTS"

report_exit "SELECT sleep(0) parked on infinite_sleep" "$SLEEP_PID"
report_exit "SYSTEM WAIT FAILPOINT dummy_pausable_failpoint PAUSE" "$WAIT_PID"

$CLICKHOUSE_CLIENT -q "SELECT name, enabled FROM system.fail_points WHERE name IN ('infinite_sleep', 'dummy_pausable_failpoint') ORDER BY name"

# --- Part 3: after DISABLE ALL a re-enabled fail point gets a fresh channel ---
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT infinite_sleep"

timeout 120 $CLICKHOUSE_CLIENT -q "SELECT sleep(0) FORMAT Null" &
SLEEP_PID=$!

timeout 120 $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT infinite_sleep PAUSE" && echo "OK: thread paused again after re-enable"

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE ALL FAILPOINTS"
report_exit "SELECT sleep(0) parked on re-enabled infinite_sleep" "$SLEEP_PID"
