#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A definition without a column list resolves its schema from the existing data during CREATE,
# and the header-dependent checks of `output_format_parquet_column_field_ids` rerun against that
# inferred schema: an unknown column or a non-covering map is rejected at CREATE time, not on the
# first INSERT. The data file path embeds the database name so that concurrent runs of this test
# do not race on a shared file.
DATA_FILE="04823_field_ids_${CLICKHOUSE_DATABASE}/data.parquet"

$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION file('${DATA_FILE}', Parquet) SELECT 1::Int64 AS x, 2::Int64 AS y SETTINGS engine_file_truncate_on_insert = 1"

# Unknown column.
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_parquet_field_ids_inf ENGINE = File(Parquet, '${DATA_FILE}') SETTINGS output_format_parquet_column_field_ids = {'missing': '1'}" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'
# The map must cover every inferred column when auto-assign is off.
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_parquet_field_ids_inf ENGINE = File(Parquet, '${DATA_FILE}') SETTINGS output_format_parquet_column_field_ids = {'x': '1'}" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'
# Same when the format is inferred too.
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_parquet_field_ids_inf ENGINE = File(auto, '${DATA_FILE}') SETTINGS output_format_parquet_column_field_ids = {'missing': '1'}" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'
# A valid map over the inferred schema is accepted.
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_parquet_field_ids_inf ENGINE = File(Parquet, '${DATA_FILE}') SETTINGS output_format_parquet_column_field_ids = {'x': '5', 'y': '7'}"
$CLICKHOUSE_CLIENT -q "SELECT * FROM t_parquet_field_ids_inf"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_parquet_field_ids_inf"

rm -rf "${USER_FILES_PATH:?}/04823_field_ids_${CLICKHOUSE_DATABASE}"
