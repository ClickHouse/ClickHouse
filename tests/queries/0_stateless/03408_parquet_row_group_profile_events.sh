#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

opts=(
  --input_format_parquet_filter_push_down=1
  --input_format_parquet_bloom_filter_push_down=1
)

USER_FILES_PATH=$($CLICKHOUSE_CLIENT_BINARY --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')

WORKING_DIR="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}"

mkdir -p "${WORKING_DIR}"

DATA_FILE="${CUR_DIR}/data_parquet/row_group.parquet"

DATA_FILE_USER_PATH="${WORKING_DIR}/row_group.parquet"

cp ${DATA_FILE} ${DATA_FILE_USER_PATH}

query_id="${CLICKHOUSE_DATABASE}_parquet_group_group_prune_profile_$RANDOM"

# A parquet file with 10 row groups from 1 to 100, 10 rows per row group.
${CLICKHOUSE_CLIENT} "${opts[@]}" --query_id="${query_id}" --query="SELECT * FROM file('${DATA_FILE_USER_PATH}', Parquet) WHERE col_int > 70 format Null" --format Null

${CLICKHOUSE_CLIENT} -nq "
  SYSTEM FLUSH LOGS query_log;

  SELECT ProfileEvents['ParquetReadRowGroups'], ProfileEvents['ParquetPrunedRowGroups']
    FROM system.query_log
	WHERE event_date >= yesterday() AND query_id = '$query_id' AND type = 'QueryFinish' and current_database = currentDatabase();
"

