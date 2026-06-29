#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_LOCAL -q "select * from numbers(10)" > $CLICKHOUSE_TMP/data.parquet
$CLICKHOUSE_LOCAL -q "select * from table" < $CLICKHOUSE_TMP/data.parquet 

$CLICKHOUSE_CLIENT -q "select * from numbers(10)" > $CLICKHOUSE_TMP/data.parquet
$CLICKHOUSE_LOCAL -q "select * from table" < $CLICKHOUSE_TMP/data.parquet 
