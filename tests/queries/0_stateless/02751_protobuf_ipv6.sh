#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

SCHEMADIR=$CURDIR/format_schemas


echo -ne '\x12\x1a\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\x01\x02\x03\x04' | $CLICKHOUSE_LOCAL --input-format Protobuf --format_schema="$SCHEMADIR/02751_protobuf_ipv6:Message" --structure="ipv6_bytes IPv6" -q "select * from table"

$CLICKHOUSE_LOCAL -q "select '::ffff:1.2.3.4'::IPv6 as ipv6_bytes format Protobuf settings format_schema = '$SCHEMADIR/02751_protobuf_ipv6:Message'" | $CLICKHOUSE_LOCAL --input-format Protobuf --format_schema="$SCHEMADIR/02751_protobuf_ipv6:Message" --structure="ipv6_bytes IPv6"  -q "select * from table"

