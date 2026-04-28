#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: Iceberg pulls in extra dependencies.
# Tag no-parallel: deliberately spawns many concurrent clients to provoke a TOCTOU race.

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/93278 (reopened).
#
# Concurrent SELECTs against an `IcebergLocal` table must not hit
#   `LOGICAL_ERROR`: 'Can't extract iceberg table state from storage snapshot'
# (STID 2606-4a47 in stress runs).
#
# Root cause: there is a TOCTOU window between `updateExternalDynamicMetadataIfExists`
# (which calls `setInMemoryMetadata` to pin `datalake_table_state`) and
# `getInMemoryMetadataPtr` later in query planning. A concurrent metadata update
# (e.g. another query's analysis phase, an INSERT producing a new snapshot, or any
# `setInMemoryMetadata` triggered by reload-on-consistency) can replace the pinned
# snapshot before the planner reads it, leaving the storage metadata snapshot
# without `datalake_table_state`. `IcebergMetadata::iterate` and
# `IcebergMetadata::isDataSortedBySortingKey` then throw `LOGICAL_ERROR`.
#
# The fix in `StorageObjectStorageSource::createFileIterator` and
# `ReadFromObjectStorageStep::requestReadingInOrder` re-fetches the snapshot from
# the configuration when it is missing, so the queries below never throw.
#
# `PR #101577` covered the materialized-view-target path (see
# `04077_iceberg_mv_select_crash.sh`); this test exercises a different path —
# concurrent normal SELECTs against the same Iceberg table — which the same fix
# also has to keep stable.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

LOG_FILE=$(mktemp -t iceberg_concurrent_XXXXXX.log)
trap "rm -f \"${LOG_FILE}\"; rm -rf \"${TABLE_PATH}\" 2>/dev/null" EXIT

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"

# `ORDER BY` makes the planner exercise the read-in-order path, so both
# `iterate` and `isDataSortedBySortingKey` are reachable from the same workload.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (c0 Int32)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    ORDER BY c0
"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 \
    --query "INSERT INTO ${TABLE} VALUES (1), (2), (3)"

# Concurrent workload designed to stress the metadata pinning / lookup path:
#  * many parallel readers — each `SELECT` calls
#    `updateExternalDynamicMetadataIfExists` (`InterpreterSelectQuery.cpp:612`)
#    which calls `setInMemoryMetadata`. Two such writers can interleave with a
#    third reader's `getInMemoryMetadataPtr` call, leaving the third reader's
#    storage snapshot with the freshly-cleared `datalake_table_state`.
#  * a slower writer thread doing serialized `INSERT`s produces new Iceberg
#    snapshots, which in turn forces `update()` and `setInMemoryMetadata`
#    inside `updateExternalDynamicMetadataIfExists` on every reader, widening
#    the TOCTOU window that the fix has to cover.
#
# The plain `SELECT` exercises `iterate` (via `createFileIterator`); the
# `SELECT … ORDER BY c0` exercises `isDataSortedBySortingKey` (via
# `requestReadingInOrder`). Both are the call sites the fix protects.
THREADS=12
ITERATIONS=10

for i in $(seq 1 $THREADS); do
    (
        for _ in $(seq 1 $ITERATIONS); do
            ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE}" >>"${LOG_FILE}" 2>&1
            ${CLICKHOUSE_CLIENT} --query "SELECT c0 FROM ${TABLE} ORDER BY c0 LIMIT 5" >>"${LOG_FILE}" 2>&1
        done
    ) &
done

# A single writer thread produces metadata churn without piling on Iceberg
# write-side conflicts (concurrent writers fight over the metadata-version file
# and would add unrelated noise to the log).
(
    for _ in $(seq 1 $((THREADS * 2))); do
        ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 \
            --query "INSERT INTO ${TABLE} VALUES (${RANDOM})" >>"${LOG_FILE}" 2>&1
    done
) &

wait

# Without the fix, at least one of the parallel queries above is liable to
# surface `Can't extract iceberg table state from storage snapshot` and the
# server logs a fatal `LOGICAL_ERROR`. With the fix, the missing
# `datalake_table_state` is repopulated from the configuration on the fly and
# every query succeeds.
if grep -q "Can't extract iceberg table state" "${LOG_FILE}"; then
    echo "FAIL: regression of #93278 (LOGICAL_ERROR seen)"
    grep "Can't extract iceberg table state" "${LOG_FILE}" | head -5
    exit 1
fi

# Final consistency check — the table must still be readable after the workload.
${CLICKHOUSE_CLIENT} --query "SELECT count() >= 3 FROM ${TABLE}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
echo "OK"
