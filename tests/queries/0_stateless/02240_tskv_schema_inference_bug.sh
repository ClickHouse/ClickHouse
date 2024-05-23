#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


USER_FILES_PATH=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')
FILE_NAME=test_02240.data
DATA_FILE=${USER_FILES_PATH:?}/$FILE_NAME

touch $DATA_FILE

echo -e 'a=1\tb=s1\tc=\N
c=[2]\ta=2\tb=\N}

a=\N
c=[3]\ta=\N'  > $DATA_FILE
$CLICKHOUSE_CLIENT --max_read_buffer_size=4 -q "desc file('$FILE_NAME', 'TSKV')"
$CLICKHOUSE_CLIENT --max_read_buffer_size=4 -q "select * from file('$FILE_NAME', 'TSKV')"

