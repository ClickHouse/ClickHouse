#!/usr/bin/env bash
# Tags: no-fasttest, no-msan, no-ubsan

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

FILE="$CURDIR/data_dwarf/tiny_dwarf5"

# The DWARF format has a fixed schema, but the caller may request other types for its columns.
# The produced columns must be converted to the requested types, otherwise every reader downstream
# interprets their memory as the wrong type.

# INSERT ... FORMAT below makes the client read stdin, so it is closed explicitly.
$CLICKHOUSE_LOCAL -m --query "
select '-- native types are returned as they are';
select toTypeName(unit_offset), unit_offset from file('$FILE', DWARF, 'offset UInt64, unit_offset LowCardinality(UInt64)') order by offset limit 1 settings allow_suspicious_low_cardinality_types = 1;
select toTypeName(tag), tag from file('$FILE', DWARF, 'offset UInt64, tag LowCardinality(String)') order by offset limit 1;

select '-- a different requested type is converted';
select toTypeName(unit_offset), unit_offset from file('$FILE', DWARF, 'offset UInt64, unit_offset UInt64') order by offset limit 1;
select toTypeName(unit_offset), unit_offset from file('$FILE', DWARF, 'offset UInt64, unit_offset String') order by offset limit 1;
select toTypeName(unit_offset), unit_offset from file('$FILE', DWARF, 'offset UInt64, unit_offset LowCardinality(String)') order by offset limit 1;
select toTypeName(size), size from file('$FILE', DWARF, 'offset UInt64, size UInt64') order by offset limit 1;
select toTypeName(tag), tag from file('$FILE', DWARF, 'offset UInt64, tag String') order by offset limit 1;
select toTypeName(name), empty(name) from file('$FILE', DWARF, 'offset UInt64, name LowCardinality(String)') order by offset limit 1;
select toTypeName(attr_str), length(attr_str) from file('$FILE', DWARF, 'offset UInt64, attr_str Array(String)') order by offset limit 1;
select toTypeName(ranges), length(ranges) from file('$FILE', DWARF, 'offset UInt64, ranges Array(Tuple(Int64, Int64))') order by offset limit 1;

select '-- inserting into a table whose column types differ from the format';
create table dwarf_dest (offset UInt64, unit_offset LowCardinality(String), attr_str Array(String)) engine = MergeTree order by offset;
insert into dwarf_dest from infile '$FILE' format DWARF;
select count() > 0, countDistinct(unit_offset) > 0 from dwarf_dest;
drop table dwarf_dest;
" < /dev/null

echo "-- a type that cannot be converted is a normal error"
$CLICKHOUSE_LOCAL -q "select * from file('$FILE', DWARF, 'unit_offset Array(UUID)')" 2>&1 | grep -o -m1 "CANNOT_CONVERT_TYPE\|ILLEGAL_TYPE_OF_ARGUMENT\|TYPE_MISMATCH"

echo "-- an unknown column name is a normal error"
$CLICKHOUSE_LOCAL -q "select * from file('$FILE', DWARF, 'bogus_column String')" 2>&1 | grep -o -m1 "THERE_IS_NO_COLUMN"
