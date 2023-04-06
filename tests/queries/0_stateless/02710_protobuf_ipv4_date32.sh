#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

SCHEMADIR=$CURDIR/format_schemas

$CLICKHOUSE_LOCAL -q "select '0.0.0.0'::IPv4 as ipv4, '2020-01-01'::Date32 as date32 format Protobuf settings format_schema = '$SCHEMADIR/02710_schema:Message'" | $CLICKHOUSE_LOCAL --input-format Protobuf --format_schema="$SCHEMADIR/02710_schema:Message" --structure="ipv4 IPv4, date32 Date32"  -q "select * from table"

