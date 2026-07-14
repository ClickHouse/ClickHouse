#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
#
# Regression test: writes through the `icebergLocal` table function created the storage
# without format settings, and `DataLakeConfiguration::write` substituted a
# default-constructed `FormatSettings{}` (`output_string_as_string = false`), so string
# columns were written as bare `BYTE_ARRAY` without the `String` annotation — an
# Iceberg-spec violation that external readers such as Spark reject.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}_tf_annotation"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

trap 'rm -rf "${TABLE_PATH}" 2>/dev/null' EXIT

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (a Int64, b String) ENGINE = IcebergLocal('${TABLE_PATH}')
"

# One insert through the table engine (control) and one through the table function
# (the previously broken path).
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg 1 --query "
    INSERT INTO ${TABLE} SELECT number, toString(number + 1) FROM numbers(10)
"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg 1 --query "
    INSERT INTO FUNCTION icebergLocal('${TABLE_PATH}') SELECT number::Int64 AS a, toString(number + 1) AS b FROM numbers(100, 100)
"

echo '--- row count ---'
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE}"

# Every data file must annotate the string column; before the fix the
# table-function-written file reported `logical_type = None` for `b`.
echo '--- string column annotations ---'
${CLICKHOUSE_LOCAL} --query "
    SELECT DISTINCT col.name AS name, col.logical_type AS logical_type
    FROM (SELECT arrayJoin(columns) AS col FROM file('${TABLE_PATH}/data/*.parquet', ParquetMetadata))
    ORDER BY name
"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
