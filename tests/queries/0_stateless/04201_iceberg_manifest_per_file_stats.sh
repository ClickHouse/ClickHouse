#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
    rm -rf "${TABLE_PATH}"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (a Int32, b Int32)
    ENGINE = IcebergLocal('${TABLE_PATH}')
    PARTITION BY (a)
"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 \
    --query "INSERT INTO ${TABLE} VALUES (1, 10), (2, 20)"

for manifest in $(find "${TABLE_PATH}/metadata" -maxdepth 1 -name '*.avro' \
        -not -name 'snap-*.avro' -type f | sort); do
    ${CLICKHOUSE_CLIENT} --query "
        SELECT
            tupleElement(data_file, 'record_count')       AS record_count,
            tupleElement(data_file, 'file_path')          AS file_path,
            tupleElement(data_file, 'file_size_in_bytes') AS file_size
        FROM file('${manifest}', Avro)
    " | while IFS=$'\t' read -r record_count file_path file_size; do
        actual_size=$(wc -c < "${file_path}" | tr -d ' ')
        echo "record_count=${record_count} file_size_matches=$([ "${file_size}" -eq "${actual_size}" ] && echo yes || echo no)"
    done
done | sort
