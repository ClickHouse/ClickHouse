#!/usr/bin/env bash
# Tags: no-parallel, long
#
# Round-17 W7 — exercise the OR-REPLACE-vs-running-refresh fence and Slot
# lifecycle. Plan reference: lucky-coalescing-codd.md items B1 + B2.
#
# What this catches:
#   - Stale refresh body publishing OLD .bin over a fresh CREATE OR REPLACE
#     (epoch fence in NamedScalarSlot::runTask). If the fence is broken,
#     the final value can be `i_old < N` rather than `N` because an old
#     body's atomic-store ran AFTER the new install.
#   - Slot lifecycle leaks (BackgroundNamedScalarRefreshPoolTask not returning to
#     0 after the storm settles, or Slot destruction deadlocking on
#     deactivate-self).
#
# Shape: tight CREATE OR REPLACE loop with REFRESH EVERY 1 SECOND. Each
# iteration writes a different SELECT constant. While the loop runs,
# refresh ticks fire on whichever entry is current; if a tick lands
# during an OR REPLACE the fence must discard the old refresh's result.
# After the loop and a settle period, the final value must equal the
# last iteration.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
CLICKHOUSE_CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_named_scalars=1"
CLICKHOUSE_LOCAL="$CLICKHOUSE_LOCAL --allow_experimental_named_scalars=1"

NAME="cv_replace_under_refresh_${CLICKHOUSE_TEST_UNIQUE_NAME:-default}"
N=100

cleanup() { ${CLICKHOUSE_CLIENT} --query "DROP NAMED SCALAR IF EXISTS ${NAME}"; }
trap cleanup EXIT
cleanup

# Initial CREATE — refresh wires up immediately. REFRESH EVERY 1 SECOND
# is fast enough that several ticks land during the loop below. The
# `now() < ...` factor produces 1 unconditionally but keeps the
# expression non-constant so the parser accepts REFRESH (constant
# expressions are explicitly rejected for refreshable scalars).
expr_for() { echo "SELECT toUInt64(now() < toDateTime('2200-01-01')) * $1"; }
${CLICKHOUSE_CLIENT} --query "CREATE NAMED SCALAR ${NAME} REFRESH EVERY 1 SECOND AS $(expr_for 0)"

# Tight loop of OR REPLACE — each iteration installs a new definition
# whose value is the iteration counter. Refresh ticks fire on whichever
# definition is current; the fence must discard any old-body publish
# that lands after the next install.
for i in $(seq 1 ${N}); do
    ${CLICKHOUSE_CLIENT} --query "CREATE OR REPLACE NAMED SCALAR ${NAME} REFRESH EVERY 1 SECOND AS $(expr_for ${i})"
done

# Let any in-flight refresh body finish and any deactivation queue settle.
# 3 seconds covers the 1-second cadence plus the longest plausible eval +
# publish window for these trivial expressions.
${CLICKHOUSE_CLIENT} --query "SELECT sleep(3) FORMAT Null"
${CLICKHOUSE_CLIENT} --query "SELECT sleep(1) FORMAT Null"

# Assertion 1: final value matches the last OR REPLACE. If the fence
# leaked, an old definition's body could have published its smaller
# constant after the last install; getNamedScalar would then return < N.
${CLICKHOUSE_CLIENT} --query "SELECT getNamedScalar('${NAME}') = toUInt64(${N}) AS final_value_is_last_create"

# Assertion 2: no orphaned refresh body. The Slot's task holder is
# deactivated synchronously by Runtime::drop / Runtime::shutdown, but a
# leak in the install path (loser slot from a non-linearizable
# installDefinition, say) could keep a ghost task firing. The dedicated
# refresh pool's task gauge — must read 0 after settle. Poll for it
# instead of a single shot to avoid flake when a refresh tick fires
# between the sleep above and this read.
gauge="1"
for _ in $(seq 1 30); do
    gauge=$(${CLICKHOUSE_CLIENT} --query "SELECT value FROM system.metrics WHERE metric = 'BackgroundNamedScalarRefreshPoolTask'")
    [ "$gauge" = "0" ] && break
    sleep 0.5
done
echo "$gauge"

# Assertion 3: the slot's bookkeeping isn't stuck mid-flight. Poll for
# refresh_in_flight == 0 to avoid catching a tick that fired between
# the metric poll above and this read.
state="1\t1\t1"
for _ in $(seq 1 30); do
    state=$(${CLICKHOUSE_CLIENT} --query "
        SELECT refresh_in_flight, has_value, current_value_is_valid
        FROM system.named_scalars
        WHERE kind = 'local' AND name = '${NAME}'")
    [ "$(printf %s "$state" | cut -f1)" = "0" ] && break
    sleep 0.5
done
printf '%s\n' "$state"
