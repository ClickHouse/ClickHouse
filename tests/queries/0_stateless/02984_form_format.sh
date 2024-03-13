#!/bin/bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Test setup 
# USER_FILES_PATH=$($CLICKHOUSE_CLIENT_BINARY --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')
USER_FILES_PATH="/home/shaun/Desktop/ClickHouse/user_files"
FILE_NAME="data.tmp"
FORM_DATA="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/${FILE_NAME}"
mkdir -p ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/
touch $FORM_DATA 

echo -ne "col1=42&col2=Hello%2C%20World%21" > $FORM_DATA

# Simple tests
$CLICKHOUSE_CLIENT -q "SELECT * from file('$FORM_DATA', Form, 'col1 UInt64, col2 String')"
$CLICKHOUSE_CLIENT -q "SELECT * from file('$FORM_DATA', Form, 'col2 String')"

# Test teardown
rm -r ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}