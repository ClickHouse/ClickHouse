#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA_FILE="data_$CLICKHOUSE_TEST_UNIQUE_NAME.csv"

$CLICKHOUSE_CLIENT --max_insert_threads=4 --query="
    EXPLAIN PIPELINE INSERT INTO FUNCTION file('$DATA_FILE') SELECT * FROM numbers_mt(1000000) ORDER BY number DESC
" | grep -o StorageFileSink | wc -l

DATA_FILE_PATH=$($CLICKHOUSE_CLIENT_BINARY --query "select _path from file('$DATA_FILE', 'One')")
rm $DATA_FILE_PATH
