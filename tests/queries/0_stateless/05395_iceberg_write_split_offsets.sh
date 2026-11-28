#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} (id Int64, s String) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet') SETTINGS output_format_parquet_row_group_size = 1000"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --max_insert_threads=1 --query "INSERT INTO ${TABLE} SELECT number, toString(number) FROM numbers(10000)"

DATA_FILE=$(find "${TABLE_PATH}data" -name '*.parquet')
MANIFEST=$(find "${TABLE_PATH}metadata" -name '*.avro' ! -name 'snap-*')

${CLICKHOUSE_CLIENT} --query "
    SELECT
        length(data_file.split_offsets) > 1,
        data_file.split_offsets = (SELECT arrayMap(rg -> toInt64(rg.file_offset), row_groups) FROM file('${DATA_FILE}', 'ParquetMetadata'))
    FROM file('${MANIFEST}', 'Avro')"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
rm -rf "${TABLE_PATH}"
