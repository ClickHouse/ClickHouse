#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
#
# Regression test: writes through the `icebergLocal` table function created the storage
# without format settings, and `DataLakeConfiguration::write` substituted a
# default-constructed `FormatSettings{}` (`output_string_as_string = false`), so string
# columns were written as bare `BYTE_ARRAY` without the `String` annotation — an
# Iceberg-spec violation that external readers such as Spark reject. Deriving the write's
# format settings from the query context also restores the requested compression codec
# (the struct-default `FormatSettings{}` compressed with snappy).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}_tf_annotation"
TABLE_TF="${TABLE}_tf"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
TF_PATH="${USER_FILES_PATH}/${TABLE_TF}/"

trap 'rm -rf "${TABLE_PATH}" "${TF_PATH}" 2>/dev/null' EXIT

# Control: a write through the table engine. The engine always carries format settings from
# the context at CREATE, so its string column has always been annotated correctly.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (a Int64, b String) ENGINE = IcebergLocal('${TABLE_PATH}')
"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg 1 --query "
    INSERT INTO ${TABLE} SELECT number, toString(number + 1) FROM numbers(10)
"

echo '--- row count ---'
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE}"

echo '--- engine-written file: string column annotation ---'
${CLICKHOUSE_LOCAL} --query "
    SELECT DISTINCT col.name AS name, col.logical_type AS logical_type
    FROM (SELECT arrayJoin(columns) AS col FROM file('${TABLE_PATH}/data/*.parquet', ParquetMetadata))
    ORDER BY name
"

# The previously broken path: a write through the icebergLocal table function into its own
# location, with a non-default codec (gzip; the format default is zstd, the struct-default
# FormatSettings{} is snappy). The fix derives the format settings from the query context, so the
# file both annotates the string column AND honors the requested codec. Before the fix this path
# used FormatSettings{}: no String annotation, snappy compression. Isolating the table-function
# write in its own location keeps the compression assertion independent of the engine control,
# whose codec comes from the CREATE-time settings. The table is created first (writing only
# metadata, no data files) because inserting through the function requires an existing table.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE_TF} (a Int64, b String) ENGINE = IcebergLocal('${TF_PATH}')
"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg 1 --output_format_parquet_compression_method gzip --query "
    INSERT INTO FUNCTION icebergLocal('${TF_PATH}') SELECT number::Int64 AS a, toString(number + 1) AS b FROM numbers(100)
"

echo '--- table-function-written file: annotation and compression ---'
${CLICKHOUSE_LOCAL} --query "
    SELECT DISTINCT col.name AS name, col.logical_type AS logical_type, col.compression AS compression
    FROM (SELECT arrayJoin(columns) AS col FROM file('${TF_PATH}/data/*.parquet', ParquetMetadata))
    ORDER BY name
"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE_TF}"
