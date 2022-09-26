#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCHEMADIR=$CURDIR/format_schemas
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -eo pipefail

# Run the client.
$CLICKHOUSE_CLIENT --multiquery <<EOF
DROP TABLE IF EXISTS no_length_delimiter_protobuf_00825;
DROP TABLE IF EXISTS roundtrip_no_length_delimiter_protobuf_00825;

CREATE TABLE no_length_delimiter_protobuf_00825
(
  x Int32,
  str String
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO no_length_delimiter_protobuf_00825 VALUES (1000, '1K'), (2000, '2K'), (3000, '3K');
SELECT * FROM no_length_delimiter_protobuf_00825;
EOF

BINARY_FILE_PATH=$(mktemp "$CURDIR/00825_protobuf_format_no_length_delimiter.XXXXXX.binary")
$CLICKHOUSE_CLIENT --query "SELECT * FROM no_length_delimiter_protobuf_00825 LIMIT 1 FORMAT ProtobufSingle SETTINGS format_schema = '$SCHEMADIR/00825_protobuf_format_no_length_delimiter:Message'" > "$BINARY_FILE_PATH"

# Check the output in the ProtobufSingle format
echo
echo "Binary representation:"
hexdump -C $BINARY_FILE_PATH

echo
(cd $SCHEMADIR && $PROTOC_BINARY --decode Message 00825_protobuf_format_no_length_delimiter.proto) < $BINARY_FILE_PATH

# Check the input in the ProtobufSingle format.
echo
echo "Roundtrip:"
$CLICKHOUSE_CLIENT --query "CREATE TABLE roundtrip_no_length_delimiter_protobuf_00825 AS no_length_delimiter_protobuf_00825"
$CLICKHOUSE_CLIENT --query "INSERT INTO roundtrip_no_length_delimiter_protobuf_00825 FORMAT ProtobufSingle SETTINGS format_schema='$SCHEMADIR/00825_protobuf_format_no_length_delimiter:Message'" < "$BINARY_FILE_PATH"
$CLICKHOUSE_CLIENT --query "SELECT * FROM roundtrip_no_length_delimiter_protobuf_00825"
rm "$BINARY_FILE_PATH"

# The ProtobufSingle format can't be used to write multiple rows because this format doesn't have any row delimiter.
$CLICKHOUSE_CLIENT --multiquery --testmode > /dev/null <<EOF
SELECT * FROM no_length_delimiter_protobuf_00825 FORMAT ProtobufSingle SETTINGS format_schema = '$SCHEMADIR/00825_protobuf_format_no_length_delimiter:Message'; -- { clientError 546 }
EOF

$CLICKHOUSE_CLIENT --multiquery <<EOF
DROP TABLE no_length_delimiter_protobuf_00825;
DROP TABLE roundtrip_no_length_delimiter_protobuf_00825;
EOF
