#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}_drop_partition"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}" >/dev/null 2>&1
    rm -rf "${TABLE_PATH}"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (partition_key Int64, value String)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    PARTITION BY partition_key
"

# Each INSERT contains one partition, so every generated manifest is
# self-contained and can be removed without rewriting surviving entries.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (1, 'one')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (2, 'two-a'), (2, 'two-b')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (3, 'three')"

SNAPSHOT_BEFORE_DROP=$(${CLICKHOUSE_CLIENT} --format TSVRaw --query "
    SELECT snapshot_id
    FROM system.iceberg_history
    WHERE database = currentDatabase() AND table = '${TABLE}'
        AND snapshot_id NOT IN
        (
            SELECT parent_id
            FROM system.iceberg_history
            WHERE database = currentDatabase() AND table = '${TABLE}' AND parent_id != 0
        )
    LIMIT 1
")

echo "before"
${CLICKHOUSE_CLIENT} --query "SELECT partition_key, value FROM ${TABLE} ORDER BY partition_key, value"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DROP PARTITION 2"

echo "after"
${CLICKHOUSE_CLIENT} --query "SELECT partition_key, value FROM ${TABLE} ORDER BY partition_key, value"

echo "time travel"
${CLICKHOUSE_CLIENT} --iceberg_snapshot_id="${SNAPSHOT_BEFORE_DROP}" \
    --query "SELECT partition_key, value FROM ${TABLE} ORDER BY partition_key, value"

echo "delete summary"
${CLICKHOUSE_CLIENT} --query "
    SELECT
        operation,
        summary['removed-data-files'],
        summary['deleted-records'],
        summary['total-data-files'],
        summary['total-records']
    FROM system.iceberg_history
    WHERE database = currentDatabase() AND table = '${TABLE}' AND operation = 'DELETE'
"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DROP PARTITION 99"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DROP PARTITION 1"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DROP PARTITION 3"

echo "empty manifest list"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
rm -rf "${TABLE_PATH}"
