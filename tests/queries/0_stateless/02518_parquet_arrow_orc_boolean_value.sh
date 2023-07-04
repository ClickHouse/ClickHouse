#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "select toBool(number % 2) as x from numbers(5) format Arrow" | $CLICKHOUSE_LOCAL --input-format=Arrow -q "select x, toTypeName(x) from table";
$CLICKHOUSE_LOCAL -q "select toBool(number % 2) as x from numbers(5) format Parquet" | $CLICKHOUSE_LOCAL --input-format=Parquet -q "select x, toTypeName(x) from table";
$CLICKHOUSE_LOCAL -q "select toBool(number % 2) as x from numbers(5) format ORC" | $CLICKHOUSE_LOCAL --input-format=ORC -q "select x, toTypeName(x) from table";

