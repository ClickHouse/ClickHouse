#!/usr/bin/env bash
# Tags: no-fasttest
# Regression test for parallel Iceberg manifest file loading.
# Verifies that `iceberg_metadata_files_parallel_loading_threads` > 1 produces
# identical results to serial loading (threads=1) across count, sum, and full
# row-level comparisons.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE="t_parallel_manifest_${CLICKHOUSE_DATABASE}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
    rm -rf "${TABLE_PATH}"
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (id UInt32, val String)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
"

# 30 separate inserts → 30 manifest files, one data file each.
for i in $(seq 1 30); do
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 \
        --query "INSERT INTO ${TABLE} VALUES (${i}, 'row_${i}')"
done

# Serial baseline
SERIAL_COUNT=$(${CLICKHOUSE_CLIENT} \
    --iceberg_metadata_files_parallel_loading_threads=1 \
    --query "SELECT count() FROM icebergLocal('${TABLE_PATH}')")

SERIAL_SUM=$(${CLICKHOUSE_CLIENT} \
    --iceberg_metadata_files_parallel_loading_threads=1 \
    --query "SELECT sum(id) FROM icebergLocal('${TABLE_PATH}')")

SERIAL_ROWS=$(${CLICKHOUSE_CLIENT} \
    --iceberg_metadata_files_parallel_loading_threads=1 \
    --query "SELECT id, val FROM icebergLocal('${TABLE_PATH}') ORDER BY id")

# Parallel run (threads=16)
PARALLEL_COUNT=$(${CLICKHOUSE_CLIENT} \
    --iceberg_metadata_files_parallel_loading_threads=16 \
    --query "SELECT count() FROM icebergLocal('${TABLE_PATH}')")

PARALLEL_SUM=$(${CLICKHOUSE_CLIENT} \
    --iceberg_metadata_files_parallel_loading_threads=16 \
    --query "SELECT sum(id) FROM icebergLocal('${TABLE_PATH}')")

PARALLEL_ROWS=$(${CLICKHOUSE_CLIENT} \
    --iceberg_metadata_files_parallel_loading_threads=16 \
    --query "SELECT id, val FROM icebergLocal('${TABLE_PATH}') ORDER BY id")

# Verify counts match
if [ "${SERIAL_COUNT}" = "${PARALLEL_COUNT}" ]; then
    echo "count OK: ${SERIAL_COUNT}"
else
    echo "FAIL count: serial=${SERIAL_COUNT} parallel=${PARALLEL_COUNT}"
fi

# Verify sums match
if [ "${SERIAL_SUM}" = "${PARALLEL_SUM}" ]; then
    echo "sum OK: ${SERIAL_SUM}"
else
    echo "FAIL sum: serial=${SERIAL_SUM} parallel=${PARALLEL_SUM}"
fi

# Verify full row content matches
if [ "${SERIAL_ROWS}" = "${PARALLEL_ROWS}" ]; then
    echo "rows OK"
else
    echo "FAIL rows differ"
    echo "--- serial ---"
    echo "${SERIAL_ROWS}"
    echo "--- parallel ---"
    echo "${PARALLEL_ROWS}"
fi
