#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

SCHEMADIR=$CURDIR/format_schemas

$CLICKHOUSE_LOCAL -q "select '0.0.0.0'::IPv4 as ipv4, ipv4 as ipv4_bytes, ipv4 as ipv4_int64, '2020-01-01'::Date32 as date32, date32 as date32_bytes, date32 as date32_int64 format Protobuf settings format_schema = '$SCHEMADIR/02710_schema:Message'" | $CLICKHOUSE_LOCAL --input-format Protobuf --format_schema="$SCHEMADIR/02710_schema:Message" --structure="ipv4 IPv4, ipv4_bytes IPv4, ipv4_int64 IPv4, date32 Date32, date32_bytes Date32, date32_int64 Date32"  -q "select * from table"

$CLICKHOUSE_LOCAL -q "select '1.2.3.4'::IPv4 as ipv4, ipv4 as ipv4_bytes, ipv4 as ipv4_int64 format Protobuf settings format_schema = '$SCHEMADIR/02710_schema:Message'" | $CLICKHOUSE_LOCAL --input-format Protobuf --format_schema="$SCHEMADIR/02710_schema:Message" --structure="ipv4 IPv4, ipv4_bytes IPv4, ipv4_int64 IPv4" -q "select * from table"

$CLICKHOUSE_LOCAL -q "select '255.255.255.255'::IPv4 as ipv4, ipv4 as ipv4_bytes, ipv4 as ipv4_int64 format Protobuf settings format_schema = '$SCHEMADIR/02710_schema:Message'" | $CLICKHOUSE_LOCAL --input-format Protobuf --format_schema="$SCHEMADIR/02710_schema:Message" --structure="ipv4 IPv4, ipv4_bytes IPv4, ipv4_int64 IPv4" -q "select * from table"



