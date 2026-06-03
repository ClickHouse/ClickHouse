#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: Iceberg pulls in extra dependencies.
# Tag no-parallel: deliberately spawns many concurrent clients to provoke a TOCTOU race.

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

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 \
    --query "INSERT INTO ${TABLE} VALUES (1), (2), (3)"

THREADS=12
ITERATIONS=10

# Plain SELECT exercises iterate (via createFileIterator); SELECT ... ORDER BY exercises
# isDataSortedBySortingKey (via requestReadingInOrder). A subshell returns non-zero as soon
# as any client fails, so a crashed/errored background query is not silently swallowed.
reader_loop() {
    for _ in $(seq 1 $ITERATIONS); do
        ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE}" >>"${LOG_FILE}" 2>&1 || return 1
        ${CLICKHOUSE_CLIENT} --query "SELECT c0 FROM ${TABLE} ORDER BY c0 LIMIT 5" >>"${LOG_FILE}" 2>&1 || return 1
    done
}

# Single writer to avoid write-side metadata-version conflicts that would add unrelated noise.
writer_loop() {
    for _ in $(seq 1 $((THREADS * 2))); do
        ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 \
            --query "INSERT INTO ${TABLE} VALUES (${RANDOM})" >>"${LOG_FILE}" 2>&1 || return 1
    done
}

declare -a PIDS=()
for _ in $(seq 1 $THREADS); do
    reader_loop &
    PIDS+=("$!")
done
writer_loop &
PIDS+=("$!")

# Wait on each PID explicitly and check its exit code; bare `wait` would return 0 and hide failures.
status=0
for pid in "${PIDS[@]}"; do
    if ! wait "$pid"; then
        status=1
    fi
done

if [ "$status" -ne 0 ]; then
    echo "FAIL: a concurrent client returned a non-zero exit code"
    grep -iE "error|exception" "${LOG_FILE}" | head -10
    exit 1
fi

# Without the fix, the missing datalake_table_state surfaces as a LOGICAL_ERROR
# "Can't extract iceberg table state from storage snapshot".
if grep -qiE "error|exception" "${LOG_FILE}"; then
    echo "FAIL: unexpected error/exception in concurrent workload"
    grep -iE "error|exception" "${LOG_FILE}" | head -10
    exit 1
fi

${CLICKHOUSE_CLIENT} --query "SELECT count() >= 3 FROM ${TABLE}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
echo "OK"
