#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the Iceberg engines depend on Avro, which is not built in fast test.
#
# A data lake table records its own `SETTINGS` clause in the settings object its configuration keeps, so
# `system.table_settings` reads the source from there rather than from the stored `CREATE` query. The two have
# to agree on `CREATE` and after the table is loaded again from what it stored.
#
# Nothing is read at `CREATE` when the columns are given, so the directory does not have to hold a table. It has
# to be under the server's user files, which the local data lake engines require.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS iceberg_definition"
$CLICKHOUSE_CLIENT -q "
CREATE TABLE iceberg_definition (a UInt64) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
SETTINGS iceberg_use_version_hint = 1"

echo "-- CREATE"
$CLICKHOUSE_CLIENT -q "
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'iceberg_definition' AND name IN ('iceberg_use_version_hint', 'iceberg_metadata_file_path')
ORDER BY name"

$CLICKHOUSE_CLIENT -q "DETACH TABLE iceberg_definition"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE iceberg_definition"

echo "-- loaded again from what it stored"
$CLICKHOUSE_CLIENT -q "
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'iceberg_definition' AND name IN ('iceberg_use_version_hint', 'iceberg_metadata_file_path')
ORDER BY name"

$CLICKHOUSE_CLIENT -q "DROP TABLE iceberg_definition"
