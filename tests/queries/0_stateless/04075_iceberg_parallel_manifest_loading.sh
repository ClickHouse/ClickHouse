#!/usr/bin/env bash
# Tags: no-fasttest
# Verifies that parallel (threads=16) and serial (threads=1) manifest loading
# produce identical results (count and sum) on a cold metadata cache.
# Branch selection / actual parallelism is verified in the integration test.

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

# Serial run (threads=1), cold cache.
${CLICKHOUSE_CLIENT} \
    --iceberg_metadata_files_parallel_loading_threads=1 \
    --use_iceberg_metadata_files_cache=0 \
    --query "SELECT count(), sum(id) FROM icebergLocal('${TABLE_PATH}')"

# Parallel run (threads=16), cold cache.
${CLICKHOUSE_CLIENT} \
    --iceberg_metadata_files_parallel_loading_threads=16 \
    --use_iceberg_metadata_files_cache=0 \
    --query "SELECT count(), sum(id) FROM icebergLocal('${TABLE_PATH}')"
