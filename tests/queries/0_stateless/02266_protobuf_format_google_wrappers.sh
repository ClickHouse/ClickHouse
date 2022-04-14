#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCHEMADIR=$CURDIR/format_schemas
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -eo pipefail

PROTOBUF_FILE_NAME="02266_protobuf_format_google_wrappers"
PROTOBUF_FILE_PATH="$SCHEMADIR/$PROTOBUF_FILE_NAME"

BINARY_FILE_PATH=$(mktemp "$CURDIR/${PROTOBUF_FILE_NAME}.XXXXXX.binary")
MESSAGE_FILE_PATH=$(mktemp "$CURDIR/${PROTOBUF_FILE_NAME}_message.XXXXXX.binary")

MAIN_TABLE="google_wrappers_02266"
ROUNDTRIP_TABLE="roundtrip_google_wrappers_02266"
COMPATIBILITY_TABLE="compatibility_google_wrappers_02266"

# takes ClickHouse format and protobuf class as arguments
format_settings() {
  clickhouse_format="$1"
  protobuf_class="$2"
  echo "FORMAT $clickhouse_format SETTINGS format_schema = '$PROTOBUF_FILE_PATH:$protobuf_class'"
}

$CLICKHOUSE_CLIENT -n --query "
  DROP TABLE IF EXISTS $MAIN_TABLE;
  DROP TABLE IF EXISTS $ROUNDTRIP_TABLE;
  DROP TABLE IF EXISTS $COMPATIBILITY_TABLE;

  CREATE TABLE $MAIN_TABLE
  (
    str Nullable(String),
    ref Int32
  ) ENGINE = MergeTree ORDER BY tuple();

  CREATE TABLE $ROUNDTRIP_TABLE AS $MAIN_TABLE;

  CREATE TABLE $COMPATIBILITY_TABLE
  (
    str Tuple(value String),
    ref Int32
  ) ENGINE = MergeTree ORDER BY tuple();
"

INITIAL_INSERT_VALUES="('str1',1),('',2),('str2',3)"
echo "Insert $INITIAL_INSERT_VALUES into table (Nullable(String), Int32):"
$CLICKHOUSE_CLIENT -n --query "
  INSERT INTO $MAIN_TABLE VALUES $INITIAL_INSERT_VALUES;
  SELECT * FROM $MAIN_TABLE;
"

echo
echo "Protobuf representation of the second row:"
$CLICKHOUSE_CLIENT --query "SELECT * FROM $MAIN_TABLE WHERE ref = 2 LIMIT 1 $(format_settings ProtobufSingle Message)" > "$BINARY_FILE_PATH"
hexdump -C $BINARY_FILE_PATH

echo
echo "Decoded with protoc:"
(cd $SCHEMADIR && $PROTOC_BINARY --decode Message "$PROTOBUF_FILE_NAME".proto) < $BINARY_FILE_PATH

echo
echo "Proto message with (NULL, 1), ('', 2), ('str', 3):"
printf '\x02\x10\x01' > "$MESSAGE_FILE_PATH" # (NULL, 1)
printf '\x04\x0A\x00\x10\x02' >> "$MESSAGE_FILE_PATH" # ('', 2)
printf '\x09\x0A\x05\x0A\x03\x73\x74\x72\x10\x03' >> "$MESSAGE_FILE_PATH" # ('str', 3)
hexdump -C $MESSAGE_FILE_PATH

echo
echo "Insert proto message into table (Nullable(String), Int32):"
$CLICKHOUSE_CLIENT --query "INSERT INTO $ROUNDTRIP_TABLE $(format_settings Protobuf Message)" < "$MESSAGE_FILE_PATH"
$CLICKHOUSE_CLIENT --query "SELECT * FROM $ROUNDTRIP_TABLE"

echo
echo "Proto output of the table:"
$CLICKHOUSE_CLIENT --query "SELECT * FROM $ROUNDTRIP_TABLE $(format_settings Protobuf Message)" > "$BINARY_FILE_PATH"
hexdump -C $BINARY_FILE_PATH

echo
echo "Proto output of the table without using Google wrappers:"
$CLICKHOUSE_CLIENT --query "SELECT * FROM $ROUNDTRIP_TABLE $(format_settings Protobuf MessageNoWrapper)" > "$BINARY_FILE_PATH"
hexdump -C $BINARY_FILE_PATH

SETTING_NAME="format_protobuf_google_wrappers_special_treatment"
echo
echo "Insert proto message into table (Tuple(String), Int32)"
echo "with disabled $SETTING_NAME:"
$CLICKHOUSE_CLIENT -n --query "
  SET $SETTING_NAME = false;
  INSERT INTO $COMPATIBILITY_TABLE $(format_settings Protobuf Message)
" < "$MESSAGE_FILE_PATH"
$CLICKHOUSE_CLIENT --query "SELECT * FROM $COMPATIBILITY_TABLE"

rm "$BINARY_FILE_PATH"
rm "$MESSAGE_FILE_PATH"

$CLICKHOUSE_CLIENT -n --query "
  DROP TABLE $MAIN_TABLE;
  DROP TABLE $ROUNDTRIP_TABLE;
  DROP TABLE $COMPATIBILITY_TABLE;
"
