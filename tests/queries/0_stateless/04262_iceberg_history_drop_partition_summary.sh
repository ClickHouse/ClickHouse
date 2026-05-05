#!/usr/bin/env bash
# Tags: no-fasttest
#
# Verify that `ALTER TABLE ... DROP PARTITION` on an Iceberg table writes a
# snapshot with `operation=DELETE` and the right `removed-*` / `total-*`
# counts to `system.iceberg_history`, and the preceding `INSERT` produces an
# `APPEND` snapshot with the matching `added-*` counts.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}_hist"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (a Int64, b String)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    PARTITION BY (a)
"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (1, 'x'), (2, 'y')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DROP PARTITION 1"

echo "=== operations in commit order ==="
${CLICKHOUSE_CLIENT} --query "
    SELECT operation
    FROM system.iceberg_history
    WHERE database = currentDatabase() AND table = '${TABLE}'
    ORDER BY made_current_at
"

echo "=== APPEND counts ==="
${CLICKHOUSE_CLIENT} --query "
    SELECT
        summary['added-data-files'],
        summary['added-records'],
        summary['total-data-files'],
        summary['total-records']
    FROM system.iceberg_history
    WHERE database = currentDatabase() AND table = '${TABLE}' AND operation = 'APPEND'
"

echo "=== DELETE counts ==="
${CLICKHOUSE_CLIENT} --query "
    SELECT
        summary['removed-data-files'],
        summary['deleted-records'],
        summary['total-data-files'],
        summary['total-records']
    FROM system.iceberg_history
    WHERE database = currentDatabase() AND table = '${TABLE}' AND operation = 'DELETE'
"

echo "=== operation key must not also appear inside summary map ==="
${CLICKHOUSE_CLIENT} --query "
    SELECT mapContains(summary, 'operation')
    FROM system.iceberg_history
    WHERE database = currentDatabase() AND table = '${TABLE}'
    ORDER BY made_current_at
"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
rm -rf "${TABLE_PATH}"
