#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE_NAME=${CLICKHOUSE_TEST_UNIQUE_NAME}.data
DATA_FILE=${USER_FILES_PATH:?}/$FILE_NAME

trap 'rm -f "$DATA_FILE"' EXIT
touch "$DATA_FILE"

echo "A one-character punctuation token is data, not a separator"

echo -e "a - 1
b - 2" > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Freeform')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'Freeform') order by all"

echo "A row after the inference sample that stops matching the solution fails instead of reusing the previous row"

for i in $(seq 1 150); do echo "abc $i"; done > $DATA_FILE
echo "short" >> $DATA_FILE

$CLICKHOUSE_CLIENT -q "select count() from file('$FILE_NAME', 'Freeform')" 2>&1 | grep -oF "CANNOT_READ_ALL_DATA" | head -1

echo "A JSON row after the inference sample that omits a key fails instead of reusing the previous row"

for i in $(seq 1 150); do echo "{\"msg\":$i,\"type\":\"log\"}"; done > $DATA_FILE
echo "{\"type\":\"log2\"}" >> $DATA_FILE

$CLICKHOUSE_CLIENT -q "select count() from file('$FILE_NAME', 'Freeform')" 2>&1 | grep -oF "CANNOT_READ_ALL_DATA" | head -1

echo "An explicit structure narrower than the inferred solution fails instead of reading out of bounds"

echo -e "1 2 3
4 5 6" > $DATA_FILE

$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'Freeform', 'c0 Int64')" 2>&1 | grep -oF "INCORRECT_NUMBER_OF_COLUMNS" | head -1

echo "An explicit structure of the right width works"

$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'Freeform', 'c0 Int64, c1 Int64, c2 Int64') order by all"
