#!/usr/bin/env bash
# Tags: no-fasttest

# Test that ProtobufList and Protobuf format behave nicely with their caches.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCHEMADIR=$CURDIR/format_schemas
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -eo pipefail

FORMAT_SCHEMA_1="$SCHEMADIR/04061_protobuf_cache_with_envelope:NumberAndSquare"
FORMAT_SCHEMA_2="$SCHEMADIR/04061_protobuf_cache_with_envelope_2:NumberAndSquare"

roundtrip()
{
    local label="$1"
    local format="$2"
    local format_schema="$3"
    local table_name="$4"

    echo "$label"
    local binary_file_path
    binary_file_path=$(mktemp "$CURDIR/04061_protobuf_cache_with_envelope.XXXXXX.binary")

    $CLICKHOUSE_CLIENT --query "SELECT * FROM squares_04061 ORDER BY number FORMAT $format SETTINGS format_schema = '$format_schema'" > "$binary_file_path"
    $CLICKHOUSE_CLIENT --query "CREATE TABLE $table_name AS squares_04061"
    $CLICKHOUSE_CLIENT --query "INSERT INTO $table_name SETTINGS format_schema = '$format_schema' FORMAT $format" < "$binary_file_path"
    $CLICKHOUSE_CLIENT --query "SELECT * FROM $table_name ORDER BY number"

    rm "$binary_file_path"
}

$CLICKHOUSE_CLIENT <<EOF
DROP TABLE IF EXISTS squares_04061;
CREATE TABLE squares_04061 (number UInt32, square UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO squares_04061 VALUES (2, 4), (0, 0), (3, 9);
EOF

# Use two different schema files with the same message name.
roundtrip "Protobuf schema 1:" Protobuf "$FORMAT_SCHEMA_1" roundtrip1_04061

roundtrip "Protobuf schema 2 after schema 1:" Protobuf "$FORMAT_SCHEMA_2" roundtrip2_04061

roundtrip "ProtobufList schema 1:" ProtobufList "$FORMAT_SCHEMA_1" roundtrip3_04061

roundtrip "ProtobufList schema 2 after schema 1:" ProtobufList "$FORMAT_SCHEMA_2" roundtrip4_04061

$CLICKHOUSE_CLIENT <<EOF
DROP TABLE squares_04061;
DROP TABLE roundtrip1_04061;
DROP TABLE roundtrip2_04061;
DROP TABLE roundtrip3_04061;
DROP TABLE roundtrip4_04061;
EOF
