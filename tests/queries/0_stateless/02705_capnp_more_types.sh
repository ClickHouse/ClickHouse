#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-replicated-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

SCHEMADIR=$CURDIR/format_schemas
$CLICKHOUSE_LOCAL -q "select 42::Int128 as int128, 42::UInt128 as uint128, 42::Int256 as int256, 42::UInt256 as uint256, 42.42::Decimal128(2) as decimal128, 42.42::Decimal256(2) as decimal256 format CapnProto settings format_schema='$SCHEMADIR/02705_big_numbers:Message'" | $CLICKHOUSE_LOCAL --input-format CapnProto --structure "int128 Int128, uint128 UInt128, int256 Int256, uint256 UInt256, decimal128 Decimal128(2), decimal256 Decimal256(2)" -q "select * from table" --format_schema="$SCHEMADIR/02705_big_numbers:Message"

$CLICKHOUSE_LOCAL -q "select map('Hello', 42, 'World', 24) as map format CapnProto settings format_schema='$SCHEMADIR/02705_map:Message'" | $CLICKHOUSE_LOCAL  --input-format CapnProto --structure "map Map(String, UInt32)" --format_schema="$SCHEMADIR/02705_map:Message"  -q "select * from table"


$CLICKHOUSE_LOCAL -q "select 42 as int8, 42 as uint8, 42 as int16, 42 as uint16, 42 as int32, 42 as uint32, 42 as int64, 42 as uint64 format CapnProto settings format_schema='$SCHEMADIR/02030_capnp_simple_types:Message'" | $CLICKHOUSE_LOCAL  --input-format CapnProto --structure "int8 UInt32, uint8 Int32, int16 Int8, uint16 UInt8, int32 UInt64, uint32 Int64, int64 UInt16, uint64 Int16" --format_schema="$SCHEMADIR/02030_capnp_simple_types:Message"  -q "select * from table"



