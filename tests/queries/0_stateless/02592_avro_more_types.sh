#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "select 42::Int128 as c1, 42::UInt128 as c2, 42::Int256 as c3, 42::UInt256 as c4, map(42, 42) as c5, toDateTime64('2020-01-01', 2) as c6 format Avro" | $CLICKHOUSE_LOCAL --input-format Avro --table test  -q "desc test"

$CLICKHOUSE_LOCAL -q "select 42::Int128 as c1, 42::UInt128 as c2, 42::Int256 as c3, 42::UInt256 as c4, map(42, 42) as c5, toDateTime64('2020-01-01', 2) as c6 format Avro" | $CLICKHOUSE_LOCAL --structure "c1 Int128, c2 UInt128, c3 Int256, c4 UInt256, c5 Map(UInt32, UInt32), c6 DateTime64(2)" --input-format Avro --table test  -q "select * from test"



