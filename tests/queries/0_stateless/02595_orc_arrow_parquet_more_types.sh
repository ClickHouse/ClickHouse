#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "select 42::Int128 as c1, 42::UInt128 as c2, 42::Int256 as c3, 42::UInt256 as c4, 'a'::Enum8('a' = 1) as c5, 'b'::Enum16('b' = 1) as c6 format Parquet" | $CLICKHOUSE_LOCAL --input-format Parquet --structure="c1 Int128, c2 UInt128, c3 Int256, c4 UInt256, c5 Enum8('a' = 1), c6 Enum16('b' = 1)"  -q "select * from table"

$CLICKHOUSE_LOCAL -q "select 42::Int128 as c1, 42::UInt128 as c2, 42::Int256 as c3, 42::UInt256 as c4, 'a'::Enum8('a' = 1) as c5, 'b'::Enum16('b' = 1) as c6 format Arrow" | $CLICKHOUSE_LOCAL --input-format Arrow --structure="c1 Int128, c2 UInt128, c3 Int256, c4 UInt256, c5 Enum8('a' = 1), c6 Enum16('b' = 1)"  -q "select * from table"

$CLICKHOUSE_LOCAL -q "select 42::Int128 as c1, 42::UInt128 as c2, 42::Int256 as c3, 42::UInt256 as c4, 'a'::Enum8('a' = 1) as c5, 'b'::Enum16('b' = 1) as c6, 42.42::Decimal256(2) as c7, '0.0.0.0'::IPv4 as c8 format ORC" | $CLICKHOUSE_LOCAL --input-format ORC --structure="c1 Int128, c2 UInt128, c3 Int256, c4 UInt256, c5 Enum8('a' = 1), c6 Enum16('b' = 1), c7 Decimal256(2), c8 IPv4"  -q "select * from table"

$CLICKHOUSE_LOCAL -q "select NULL::Nullable(IPv6) as x format ORC" | $CLICKHOUSE_LOCAL --input-format ORC --structure="x Nullable(IPv6)"  -q "select * from table"

$CLICKHOUSE_LOCAL -q "select NULL::Nullable(UInt256) as x format ORC" | $CLICKHOUSE_LOCAL --input-format ORC --structure="x Nullable(UInt256)"  -q "select * from table"

