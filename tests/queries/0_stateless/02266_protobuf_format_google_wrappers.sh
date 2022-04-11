#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCHEMADIR=$CURDIR/format_schemas
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -eo pipefail

CLICKHOUSE_CLIENT="/home/jk/work/tools/ClickHouse/build/programs/${CLICKHOUSE_CLIENT}"
BINARY_FILE_PATH=$(mktemp "$CURDIR/02266_protobuf_format_google_wrappers.XXXXXX.binary")

# Run the client.
echo "Table:"
$CLICKHOUSE_CLIENT --multiquery <<EOF
DROP TABLE IF EXISTS google_wrappers_02266;
DROP TABLE IF EXISTS roundtrip_google_wrappers_02266;

CREATE TABLE google_wrappers_02266
(
  str Nullable(String),
  ref Int32
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO google_wrappers_02266 VALUES ('str1', 1), ('', 2), ('str2', 3);
SELECT * FROM google_wrappers_02266;
EOF

$CLICKHOUSE_CLIENT --query "SELECT * FROM google_wrappers_02266 WHERE ref = 2 LIMIT 1 FORMAT ProtobufSingle SETTINGS format_schema = '$SCHEMADIR/02266_protobuf_format_google_wrappers:Message'" > "$BINARY_FILE_PATH"

# Check the output in the ProtobufSingle format
echo
echo "Binary representation of second row:"
hexdump -C $BINARY_FILE_PATH

echo
echo "Decoded:"
(cd $SCHEMADIR && $PROTOC_BINARY --decode Message 02266_protobuf_format_google_wrappers.proto) < $BINARY_FILE_PATH

echo
echo "Proto message with (NULL, 1), ('', 2), ('str', 3):"
printf '\x02\x10\x01' > "$BINARY_FILE_PATH" # (NULL, 1)
printf '\x04\x0A\x00\x10\x02' >> "$BINARY_FILE_PATH" # ('', 2)
printf '\x09\x0A\x05\x0A\x03\x73\x74\x72\x10\x03' >> "$BINARY_FILE_PATH" # ('str', 3)
hexdump -C $BINARY_FILE_PATH

echo
echo "Table after putting the message in:"
$CLICKHOUSE_CLIENT --query "CREATE TABLE roundtrip_google_wrappers_02266 AS google_wrappers_02266"
$CLICKHOUSE_CLIENT --query "INSERT INTO roundtrip_google_wrappers_02266 FORMAT Protobuf SETTINGS format_schema='$SCHEMADIR/02266_protobuf_format_google_wrappers:Message'" < "$BINARY_FILE_PATH"
$CLICKHOUSE_CLIENT --query "SELECT * FROM roundtrip_google_wrappers_02266"

# TODO: it outputs the null string as if it was empty, to investigate
$CLICKHOUSE_CLIENT --query "SELECT * FROM roundtrip_google_wrappers_02266 FORMAT Protobuf SETTINGS format_schema = '$SCHEMADIR/02266_protobuf_format_google_wrappers:Message'" > "$BINARY_FILE_PATH"
echo
echo "Proto output of the table:"
hexdump -C $BINARY_FILE_PATH

$CLICKHOUSE_CLIENT --query "SELECT * FROM roundtrip_google_wrappers_02266 FORMAT Protobuf SETTINGS format_schema = '$SCHEMADIR/02266_protobuf_format_google_wrappers:MessageNoWrapper'" > "$BINARY_FILE_PATH"
echo
echo "Proto output of the table without using wrapper:"
hexdump -C $BINARY_FILE_PATH

rm "$BINARY_FILE_PATH"

$CLICKHOUSE_CLIENT --multiquery <<EOF
DROP TABLE google_wrappers_02266;
DROP TABLE roundtrip_google_wrappers_02266;
EOF
