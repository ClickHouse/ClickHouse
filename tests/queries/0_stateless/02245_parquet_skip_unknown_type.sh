#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER_FILES_PATH=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')
FILE_NAME=test_02245.parquet
DATA_FILE=$USER_FILES_PATH/$FILE_NAME

cp $CUR_DIR/data_parquet_bad_column/metadata_0.parquet $DATA_FILE 


$CLICKHOUSE_CLIENT -q "desc file(test_02245.parquet)" 2>&1 | grep -qF "CANNOT_EXTRACT_TABLE_STRUCTURE" && echo "OK" || echo "FAIL"
$CLICKHOUSE_CLIENT -q "desc file(test_02245.parquet) settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference=1"
$CLICKHOUSE_CLIENT -q "select count(*) from file(test_02245.parquet) settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference=1"

