#!/usr/bin/env bash
# Tags: no-fasttest
#       ^ no Parquet support in fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "select tuple(10, 20) as x format Parquet" |  $CLICKHOUSE_LOCAL --input-format=Parquet --table test -q "select * from test"
$CLICKHOUSE_LOCAL -q "select (10, 10)::Point format Parquet" |  $CLICKHOUSE_LOCAL --input-format=Parquet --table test -q "select * from test"
$CLICKHOUSE_LOCAL -q "select CAST(([1, 2, 3], ['Ready', 'Steady', 'Go']), 'Map(UInt8, String)') AS map format Parquet" |  $CLICKHOUSE_LOCAL --input-format=Parquet --table test -q "select * from test"
