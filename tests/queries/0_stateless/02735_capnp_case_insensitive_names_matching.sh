#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-replicated-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

SCHEMADIR=$CURDIR/format_schemas
$CLICKHOUSE_LOCAL -q "select 42 as Field1, (42, 42)::Tuple(Field1 UInt32, Field2 UInt32) as Nested format CapnProto settings format_schema='$SCHEMADIR/02735_case_insensitive_names_matching:Message'" | $CLICKHOUSE_LOCAL --input-format CapnProto --structure "Field1 UInt32, Nested Tuple(Field1 UInt32, Field2 UInt32)" -q "select * from table" --format_schema="$SCHEMADIR/02735_case_insensitive_names_matching:Message"

