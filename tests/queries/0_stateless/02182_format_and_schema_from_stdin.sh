#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT -q "select * from numbers(10) format Parquet" > $CLICKHOUSE_TMP/data.parquet
$CLICKHOUSE_LOCAL -q "select * from table" --file="-" < $CLICKHOUSE_TMP/data.parquet 

echo "1,\"String\", \"[1, 2, 3]\"" | $CLICKHOUSE_LOCAL -q "select * from table" --input-format=CSV

