#!/usr/bin/env bash
# Tags: no-fasttest, no-cpu-aarch64

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA_FILE=$CLICKHOUSE_TEST_UNIQUE_NAME.csv
$CLICKHOUSE_LOCAL -q "select number > 1000000 ? 'error' : toString(number) from numbers(2000000) format CSV" > $DATA_FILE
$CLICKHOUSE_LOCAL -q "select * from file($DATA_FILE, CSV, 'x UInt64') format Null settings input_format_allow_errors_ratio=1"
rm $DATA_FILE
