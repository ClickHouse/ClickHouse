#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
SCHEMADIR=$CURDIR/format_schemas

$CLICKHOUSE_LOCAL -q "select tuple(42, 'Hello', [1,2,3]) as x format Protobuf settings format_schema='$SCHEMADIR/02707_schema:Message'" | $CLICKHOUSE_LOCAL --input-format Protobuf --structure='x Tuple(UInt32, String, Array(UInt32))' -q "select * from table" --format_schema="$SCHEMADIR/02707_schema:Message"

