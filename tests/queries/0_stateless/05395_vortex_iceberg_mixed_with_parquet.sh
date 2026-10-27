#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# ^ the Vortex format is not included in the fast test and MSan builds, and `IcebergLocal` needs
# the USE_AVRO build option

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The data files of an Iceberg snapshot can be in different formats, and their readers share the
# resources of one `FormatParserSharedResources`. The `Parquet` reader must find its own state
# there whether a `Vortex` reader or a `Parquet` one initialized it first, so both orders of
# writing are read, with one stream and with several.

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/05395_vortex_iceberg_XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

# `clickhouse-local` takes its working directory for the user files directory.
cd "${LOCAL_DIR}" || exit 1

${CLICKHOUSE_LOCAL} --allow_insert_into_iceberg 1 --query "
    CREATE TABLE vortex_first (a Int64, s String) ENGINE = IcebergLocal('${LOCAL_DIR}/vortex_first/', 'Vortex');
    INSERT INTO vortex_first SELECT number, toString(number) FROM numbers(1000);
    CREATE TABLE parquet_first (a Int64, s String) ENGINE = IcebergLocal('${LOCAL_DIR}/parquet_first/', 'Parquet');
    INSERT INTO parquet_first SELECT number, toString(number) FROM numbers(1000);
"

${CLICKHOUSE_LOCAL} --allow_insert_into_iceberg 1 --query "
    CREATE TABLE vortex_first ENGINE = IcebergLocal('${LOCAL_DIR}/vortex_first/', 'Parquet');
    INSERT INTO vortex_first SELECT number, toString(number) FROM numbers(1000, 1000);
    CREATE TABLE parquet_first ENGINE = IcebergLocal('${LOCAL_DIR}/parquet_first/', 'Vortex');
    INSERT INTO parquet_first SELECT number, toString(number) FROM numbers(1000, 1000);
"

for table in vortex_first parquet_first; do
    for threads in 1 4; do
        echo "-- $table, max_threads = $threads"
        ${CLICKHOUSE_LOCAL} --query "
            CREATE TABLE $table ENGINE = IcebergLocal('${LOCAL_DIR}/$table/', 'Parquet');
            SELECT count(), sum(a), max(s) FROM $table SETTINGS max_threads = $threads;
            SELECT countIf(_path LIKE '%.vortex'), countIf(_path LIKE '%.parquet') FROM $table SETTINGS max_threads = $threads;"
    done
done
