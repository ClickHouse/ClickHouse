#!/usr/bin/env bash
# Tags: no-ubsan, no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER_FILES_PATH=$($CLICKHOUSE_CLIENT_BINARY --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')

WORKING_DIR="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}"

mkdir -p "${WORKING_DIR}"

DATA_FILE="${CUR_DIR}/data_parquet/nullbool.parquet"

DATA_FILE_USER_PATH="${WORKING_DIR}/nullbool.parquet"

cp ${DATA_FILE} ${DATA_FILE_USER_PATH}

${CLICKHOUSE_CLIENT} --query="select id, bool from file('${DATA_FILE_USER_PATH}', Parquet) order by id SETTINGS input_format_parquet_use_native_reader=false;"
${CLICKHOUSE_CLIENT} --query="select id, bool from file('${DATA_FILE_USER_PATH}', Parquet) order by id SETTINGS input_format_parquet_use_native_reader=true;"
