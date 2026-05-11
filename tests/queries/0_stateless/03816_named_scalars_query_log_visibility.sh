#!/usr/bin/env bash
# Tags: no-parallel, long
#
# Refresh-body visibility & cancellation. Asserts that:
#   1. A SYSTEM REFRESH NAMED SCALAR shows up in system.query_log with is_internal=1.
#   2. While a slow refresh is in flight, system.processes lists it (KILL QUERY targets it).
#   3. KILL QUERY ... SYNC actually interrupts the refresh and the scalar
#      records last_error_type = 'QUERY_WAS_CANCELLED'.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
CLICKHOUSE_CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_named_scalars=1"

NAME="cv_visibility_${CLICKHOUSE_TEST_UNIQUE_NAME:-default}"
SLOW_NAME="cv_slow_${CLICKHOUSE_TEST_UNIQUE_NAME:-default}"

cleanup() {
    ${CLICKHOUSE_CLIENT} -q "DROP NAMED SCALAR IF EXISTS ${NAME}"
    ${CLICKHOUSE_CLIENT} -q "DROP NAMED SCALAR IF EXISTS ${SLOW_NAME}"
}
trap cleanup EXIT
cleanup

# --- 1) refresh body lands in system.query_log with is_internal=1 ----------
${CLICKHOUSE_CLIENT} -q "CREATE NAMED SCALAR ${NAME} REFRESH EVERY 36500 DAYS AS SELECT toUInt64(now() < toDateTime('2200-01-01')) * 12345"
${CLICKHOUSE_CLIENT} -q "SYSTEM REFRESH NAMED SCALAR ${NAME}"
${CLICKHOUSE_CLIENT} -q "SELECT getNamedScalar('${NAME}')"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} -q "
    SELECT count() >= 1 AS refresh_in_query_log
    FROM system.query_log
    WHERE is_internal = 1
      AND type = 'QueryFinish'
      AND query LIKE '%12345%'
      AND query NOT LIKE '%FROM system.query_log%'
"

# --- 2 + 3) KILL QUERY interrupts a running refresh -------------------------
# Block-aligned sleep so KILL QUERY can interrupt between blocks (default
# block size is 65k; sleep fires per block).
${CLICKHOUSE_CLIENT} -q "CREATE NAMED SCALAR ${SLOW_NAME} REFRESH EVERY 36500 DAYS AS SELECT min(number) FROM numbers(10000000) WHERE NOT(ignore(sleep(0.1)))"
# Kick a refresh and immediately try to kill it. The refresh fires asynchronously
# on the background pool; poll until system.processes shows it, then kill.
${CLICKHOUSE_CLIENT} -q "SYSTEM REFRESH NAMED SCALAR ${SLOW_NAME}"

found=""
for _ in $(seq 1 60); do
    found=$(${CLICKHOUSE_CLIENT} -q "
        SELECT count() FROM system.processes
        WHERE query LIKE '%numbers(10000000)%' AND is_initial_query = 0
    ")
    [ "$found" = "1" ] && break
    sleep 0.1
done
echo "saw_refresh_in_processes=${found}"

${CLICKHOUSE_CLIENT} -q "KILL QUERY WHERE query LIKE '%numbers(10000000)%' SYNC FORMAT Null" || true

# Wait for the named-scalar refresh task to record the cancellation.
cancel_recorded="0"
for _ in $(seq 1 80); do
    cancel_recorded=$(${CLICKHOUSE_CLIENT} -q "
        SELECT coalesce(exception, '') LIKE '%QUERY_WAS_CANCELLED%'
        FROM system.named_scalars
        WHERE name = '${SLOW_NAME}'
    ")
    [ "$cancel_recorded" = "1" ] && break
    sleep 0.1
done
echo "cancel_recorded=${cancel_recorded}"
