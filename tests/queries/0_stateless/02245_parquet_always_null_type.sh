#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE_NAME=test_02245.parquet
DATA_FILE=$USER_FILES_PATH/$FILE_NAME

cp $CUR_DIR/data_parquet_bad_column/metadata_0.parquet $DATA_FILE

# The native reader supports the always-null Parquet type.
$CLICKHOUSE_CLIENT -q "desc file(test_02245.parquet)"
$CLICKHOUSE_CLIENT -q "select count(*) from file(test_02245.parquet)"
