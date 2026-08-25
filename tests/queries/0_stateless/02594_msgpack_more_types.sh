#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "select 'a'::Enum8('a' = 1) as c1, 'b'::Enum16('b' = 1) as c2, '2020-01-01'::Date32 as c3, 42::Int128 as c4, 42::UInt128 as c5, 42::Int256 as c6, 42::UInt256 as c7, 42.42::Decimal32(2) as c8, 42.42::Decimal64(2) as c9, 42.42::Decimal128(2) as c10, 42.42::Decimal256(2) as c11 format MsgPack" | $CLICKHOUSE_LOCAL --input-format MsgPack --structure="c1 Enum8('a' = 1), c2 Enum16('b' = 1), c3 Date32, c4 Int128, c5 UInt128, c6 Int256, c7 UInt256, c8 Decimal32(2), c9 Decimal64(2), c10 Decimal128(2), c11 Decimal256(2)" -q "select * from table"

$CLICKHOUSE_LOCAL -q "select tuple(42, 'Hello') as c1, tuple(map(42, [1, 2, 3]), [tuple([tuple(1, 2), tuple(1, 2)], 'Hello', [1, 2, 3]), tuple([], 'World', [1])]) as c2 format MsgPack" | $CLICKHOUSE_LOCAL --input-format MsgPack --structure="c1 Tuple(UInt32, String), c2 Tuple(Map(UInt32, Array(UInt32)), Array(Tuple(Array(Tuple(UInt32, UInt32)), String, Array(UInt32))))" -q "select * from table"

