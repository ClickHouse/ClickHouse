#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER_FILES_PATH=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')
cp $CUR_DIR/data_orc/corrupted.orc $USER_FILES_PATH/

${CLICKHOUSE_CLIENT} --query="select * from file('corrupted.orc', 'ORC', 'x UInt32')" 2>&1 | grep -F -q 'Footer is corrupt' && echo 'OK' || echo 'FAIL'

