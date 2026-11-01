#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, long
# Tag no-fasttest: Iceberg pulls in extra dependencies.
# Tag no-parallel: deliberately runs concurrent clients to provoke a TOCTOU race.
# Tag long: the concurrent read/write loop can run past the 180s flaky-check cap under msan;
# shrinking it would weaken the concurrency that provokes the race, so exempt it instead.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

LOG_FILE=$(mktemp -t iceberg_concurrent_XXXXXX.log)
trap "rm -f \"${LOG_FILE}\"; rm -rf \"${TABLE_PATH}\" 2>/dev/null" EXIT

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"

# ORDER BY makes the planner exercise the read-in-order path, so both iterate and
# isDataSortedBySortingKey are reachable from the same workload.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (c0 Int32)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    ORDER BY c0
"

# </dev/null: clickhouse-client reads stdin for trailing INSERT data; an inherited
# non-EOF stdin would block the INSERT (applies to every client invocation below too).
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 \
    --query "INSERT INTO ${TABLE} VALUES (1), (2), (3)" </dev/null

READERS=6
READS_PER_CLIENT=15
WRITES=20

# Each reader/writer runs its whole loop inside ONE client connection (one process spawn),
# instead of spawning a fresh client per query. Every statement still re-resolves the Iceberg
# table-state snapshot independently (verified: a later SELECT in the same connection sees rows
# the writer committed after an earlier SELECT), so the concurrent-commit TOCTOU race is fully
# preserved while the per-query process/settings-parse overhead is removed -- on a debug build
# with randomized settings the old per-query spawning pushed runtime over the 180s test limit.

# Plain SELECT exercises iterate (via createFileIterator); SELECT ... ORDER BY exercises
# isDataSortedBySortingKey (via requestReadingInOrder). Build the read script once.
read_script=""
for _ in $(seq 1 $READS_PER_CLIENT); do
    read_script+="SELECT count() FROM ${TABLE} FORMAT Null;"
    read_script+="SELECT c0 FROM ${TABLE} ORDER BY c0 LIMIT 5 FORMAT Null;"
done

# Single writer to avoid write-side metadata-version conflicts that would add unrelated noise.
write_script=""
for i in $(seq 1 $WRITES); do
    write_script+="INSERT INTO ${TABLE} VALUES (${i});"
done

for _ in $(seq 1 $READERS); do
    ${CLICKHOUSE_CLIENT} --query "${read_script}" >>"${LOG_FILE}" 2>&1 </dev/null &
done
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "${write_script}" >>"${LOG_FILE}" 2>&1 </dev/null &

# Wait for all concurrent clients. We deliberately do NOT fail on an individual client's
# non-zero exit: concurrent metadata commits can make an unrelated reader hit a benign,
# catchable error (e.g. INCORRECT_DATA reading a partially written metadata.json) that is
# not the regression under test and does not crash the server. The guarded regression is
# detected positively below, via both of its manifestations.
wait

status=0

# Debug/sanitizer: the "Can't extract iceberg table state" LOGICAL_ERROR is fatal under
# abort_on_logical_error (forced on there), so it aborts the server and the string reaches
# the server log, not the client log. Detect the abort by probing liveness (retried once to
# ignore a transient blip on an otherwise healthy server).
if ! ${CLICKHOUSE_CLIENT} --query "SELECT 1" >/dev/null 2>&1; then
    sleep 1
    if ! ${CLICKHOUSE_CLIENT} --query "SELECT 1" >/dev/null 2>&1; then
        echo "FAIL: server not responding after concurrent Iceberg access (possible LOGICAL_ERROR abort)"
        status=1
    fi
fi

# Release: the same LOGICAL_ERROR is a catchable exception carrying the exact message to the
# client. Deterministic counterpart: 04305_iceberg_missing_table_state.sh.
if grep -qF "Can't extract iceberg table state" "${LOG_FILE}"; then
    echo "FAIL: observed the 'Can't extract iceberg table state' LOGICAL_ERROR"
    status=1
fi

if [ "$status" -ne 0 ]; then
    cat "${LOG_FILE}"
    exit 1
fi

${CLICKHOUSE_CLIENT} --query "SELECT count() >= 3 FROM ${TABLE}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
echo "OK"
