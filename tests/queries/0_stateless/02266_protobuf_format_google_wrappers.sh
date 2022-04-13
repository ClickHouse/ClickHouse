#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCHEMADIR=$CURDIR/format_schemas
. "$CURDIR"/../shell_config.sh

set -eo pipefail

BINARY_FILE_PATH=$(mktemp "$CURDIR/02266_protobuf_format_google_wrappers.XXXXXX.binary")
MESSAGE_FILE_PATH=$(mktemp "$CURDIR/02266_protobuf_format_google_wrappers_message.XXXXXX.binary")

echo "Table (Nullable(String), Int32):"
$CLICKHOUSE_CLIENT --multiquery <<EOF
DROP TABLE IF EXISTS google_wrappers_02266;
DROP TABLE IF EXISTS roundtrip_google_wrappers_02266;
DROP TABLE IF EXISTS before_google_wrappers_02266;

CREATE TABLE google_wrappers_02266
(
  str Nullable(String),
  ref Int32
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO google_wrappers_02266 VALUES ('str1', 1), ('', 2), ('str2', 3);
SELECT * FROM google_wrappers_02266;

CREATE TABLE before_google_wrappers_02266
(
  str Tuple(value String),
  ref Int32
) ENGINE = MergeTree ORDER BY tuple();

CREATE TABLE roundtrip_google_wrappers_02266 AS google_wrappers_02266
EOF

$CLICKHOUSE_CLIENT --query "SELECT * FROM google_wrappers_02266 WHERE ref = 2 LIMIT 1 FORMAT ProtobufSingle SETTINGS format_schema = '$SCHEMADIR/02266_protobuf_format_google_wrappers:Message'" > "$BINARY_FILE_PATH"

echo
echo "Protobuf representation of the second row:"
hexdump -C $BINARY_FILE_PATH

echo
echo "Decoded:"
(cd $SCHEMADIR && $PROTOC_BINARY --decode Message 02266_protobuf_format_google_wrappers.proto) < $BINARY_FILE_PATH

echo
echo "Proto message with (NULL, 1), ('', 2), ('str', 3):"
printf '\x02\x10\x01' > "$MESSAGE_FILE_PATH" # (NULL, 1)
printf '\x04\x0A\x00\x10\x02' >> "$MESSAGE_FILE_PATH" # ('', 2)
printf '\x09\x0A\x05\x0A\x03\x73\x74\x72\x10\x03' >> "$MESSAGE_FILE_PATH" # ('str', 3)
hexdump -C $MESSAGE_FILE_PATH

echo
echo "Initial table after putting the message in:"
$CLICKHOUSE_CLIENT --query "INSERT INTO roundtrip_google_wrappers_02266 FORMAT Protobuf SETTINGS format_schema='$SCHEMADIR/02266_protobuf_format_google_wrappers:Message'" < "$MESSAGE_FILE_PATH"
$CLICKHOUSE_CLIENT --query "SELECT * FROM roundtrip_google_wrappers_02266"

$CLICKHOUSE_CLIENT --query "SELECT * FROM roundtrip_google_wrappers_02266 FORMAT Protobuf SETTINGS format_schema = '$SCHEMADIR/02266_protobuf_format_google_wrappers:Message'" > "$BINARY_FILE_PATH"
echo
echo "Proto output of the table:"
hexdump -C $BINARY_FILE_PATH

$CLICKHOUSE_CLIENT --query "SELECT * FROM roundtrip_google_wrappers_02266 FORMAT Protobuf SETTINGS format_schema = '$SCHEMADIR/02266_protobuf_format_google_wrappers:MessageNoWrapper'" > "$BINARY_FILE_PATH"
echo
echo "Proto output of the table without using Google wrappers:"
hexdump -C $BINARY_FILE_PATH

echo
echo "Inserting message into table (Tuple(String), Int32)"
echo "with disabled format_protobuf_google_wrappers_special_treatment:"
$CLICKHOUSE_CLIENT -n --query "SET format_protobuf_google_wrappers_special_treatment = false; INSERT INTO before_google_wrappers_02266 FORMAT Protobuf SETTINGS format_schema='$SCHEMADIR/02266_protobuf_format_google_wrappers:Message'" < "$MESSAGE_FILE_PATH"
$CLICKHOUSE_CLIENT --query "SELECT * FROM before_google_wrappers_02266"

rm "$BINARY_FILE_PATH"
rm "$MESSAGE_FILE_PATH"

$CLICKHOUSE_CLIENT --multiquery <<EOF
DROP TABLE google_wrappers_02266;
DROP TABLE roundtrip_google_wrappers_02266;
DROP TABLE before_google_wrappers_02266;
EOF
