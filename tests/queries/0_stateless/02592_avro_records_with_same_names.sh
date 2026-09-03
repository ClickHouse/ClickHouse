#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "select tuple(tuple(1, 2))::Tuple(x Tuple(a UInt32, b UInt32)) as c1, tuple(tuple(3, 4, 5))::Tuple(x Tuple(c UInt32, d UInt32, e UInt32)) as c2 format Avro" | $CLICKHOUSE_LOCAL --input-format Avro --structure 'c1 Tuple(x Tuple(a UInt32, b UInt32)), c2 Tuple(x Tuple(c UInt32, d UInt32, e UInt32))' -q "select * from table"
