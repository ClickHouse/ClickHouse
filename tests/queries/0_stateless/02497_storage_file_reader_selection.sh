#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

USER_FILES_PATH=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')

DATA_FILE=$USER_FILES_PATH/test_02497_storage_file_reader.data
echo 'key\nfoo\nbar' > $DATA_FILE

QUERY_ID=$RANDOM
$CLICKHOUSE_CLIENT -q "SELECT * FROM file('test_02497_storage_file_reader.data', 'TSV', 's String')" \
  --query_id $QUERY_ID \
  --local_filesystem_read_method=mmap \
  --min_bytes_to_use_mmap_io=1 \

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS"
$CLICKHOUSE_CLIENT -q "SELECT sum(ProfileEvents['CreatedReadBufferMMap']) FROM system.query_log WHERE query_id='$QUERY_ID'"
$CLICKHOUSE_CLIENT -q "SELECT sum(ProfileEvents['CreatedReadBufferOrdinary']) FROM system.query_log WHERE query_id='$QUERY_ID'"

QUERY_ID=$RANDOM
$CLICKHOUSE_CLIENT -q "SELECT * FROM file('test_02497_storage_file_reader.data', 'TSV', 's String')" \
  --query_id $QUERY_ID \
  --local_filesystem_read_method=pread

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS"
$CLICKHOUSE_CLIENT -q "SELECT sum(ProfileEvents['CreatedReadBufferMMap']) FROM system.query_log WHERE query_id='$QUERY_ID'"
$CLICKHOUSE_CLIENT -q "SELECT sum(ProfileEvents['CreatedReadBufferOrdinary']) FROM system.query_log WHERE query_id='$QUERY_ID'"
