#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCHEMADIR=$CURDIR/format_schemas
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "select [[[42, 42], [], [42]], [[], [42], [42, 42, 42, 42]]] as a format Protobuf settings format_schema = '$SCHEMADIR/00825_protobuf_format_array_3dim:ABC'" | $CLICKHOUSE_LOCAL --input-format Protobuf --format_schema="$SCHEMADIR/00825_protobuf_format_array_3dim:ABC" --structure="a Array(Array(Array(Int32)))"  -q "select * from table"

