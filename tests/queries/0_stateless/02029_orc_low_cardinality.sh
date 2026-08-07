#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS orc_lc";


$CLICKHOUSE_CLIENT --query="CREATE TABLE orc_lc (lc LowCardinality(String), array_lc Array(LowCardinality(String)), tuple_lc  Tuple(LowCardinality(String))) ENGINE = Memory()";


$CLICKHOUSE_CLIENT --query="SELECT [lc] as array_lc, tuple(lc) as tuple_lc, toLowCardinality(toString(number)) as lc from numbers(10) FORMAT ORC" | $CLICKHOUSE_CLIENT --query="INSERT INTO orc_lc FORMAT ORC";

$CLICKHOUSE_CLIENT --query="SELECT * FROM orc_lc";
