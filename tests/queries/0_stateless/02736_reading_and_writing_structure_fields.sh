#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-replicated-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

SCHEMADIR=$CURDIR/format_schemas
DATA_FILE=02736_$CLICKHOUSE_TEST_UNIQUE_NAME.bin

$CLICKHOUSE_LOCAL -q "select tuple(42, tuple(42, 42), [tuple(42, 42), tuple(24, 24)]) as nested, [tuple(42, tuple(42, 42), [tuple(42, 42), tuple(24, 24)]), tuple(24, tuple(24, 24), [tuple(24, 24), tuple(42, 42)])] as nestedList format CapnProto settings format_schema='$SCHEMADIR/02736_nested_structures:Message'" > $DATA_FILE

$CLICKHOUSE_LOCAL -q "select * from file($DATA_FILE, CapnProto) settings format_schema='$SCHEMADIR/02736_nested_structures:Message'"

$CLICKHOUSE_LOCAL -q "select 42 as nested_field1, 42 as nested_nested_field1, 42 as nested_nested_field2 format CapnProto settings format_schema='$SCHEMADIR/02736_nested_structures:Message'" > $DATA_FILE

$CLICKHOUSE_LOCAL -q "select * from file($DATA_FILE, CapnProto, 'nested_field1 UInt32, nested_nested_field1 UInt32, nested_nested_field2 UInt32') settings format_schema='$SCHEMADIR/02736_nested_structures:Message'"

$CLICKHOUSE_LOCAL -q "select [42, 24] as nestedList_field1, [42, 24] as nestedList_nested_field1, [42, 24] as nestedList_nested_field2, [[42, 24], [24, 42]] as nestedList_nestedList_field1, [[42, 24], [24, 42]] as nestedList_nestedList_field2 format CapnProto settings format_schema='$SCHEMADIR/02736_nested_structures:Message'" > $DATA_FILE

$CLICKHOUSE_LOCAL -q "select * from file($DATA_FILE, CapnProto, 'nestedList_field1 Array(UInt32), nestedList_nested_field1 Array(UInt32), nestedList_nested_field2 Array(UInt32), nestedList_nestedList_field1 Array(Array(UInt32)), nestedList_nestedList_field2 Array(Array(UInt32))') settings format_schema='$SCHEMADIR/02736_nested_structures:Message'"

rm $DATA_FILE

