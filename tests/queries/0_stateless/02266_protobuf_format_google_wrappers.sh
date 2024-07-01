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
MULTI_TABLE="multi_google_wrappers_02266"

INPUT_SETTING="input_format_protobuf_flatten_google_wrappers"
OUTPUT_SETTING="output_format_protobuf_nullables_with_google_wrappers"

SET_INPUT="SET $INPUT_SETTING = true;"
SET_OUTPUT="SET $OUTPUT_SETTING = true;"

INITIAL_INSERT_VALUES="('str1',1),('',2),('str2',3)"
MULTI_WRAPPER_VALUES="(0,1,2)"

# takes ClickHouse format and protobuf class as arguments
protobuf_info() {
  input_or_output="$1"
  clickhouse_format="$2"
  protobuf_class="$3"

  format_part="FORMAT $clickhouse_format"
  settings_part="SETTINGS format_schema = '$PROTOBUF_FILE_PATH:$protobuf_class'"

  if [ "$input_or_output" == "input" ]; then
    echo "$settings_part $format_part"
  else
    echo "$format_part $settings_part"
  fi
}

$CLICKHOUSE_CLIENT -n --query "
  DROP TABLE IF EXISTS $MAIN_TABLE;
  DROP TABLE IF EXISTS $ROUNDTRIP_TABLE;
  DROP TABLE IF EXISTS $COMPATIBILITY_TABLE;
  DROP TABLE IF EXISTS $MULTI_TABLE;

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

  CREATE TABLE $MULTI_TABLE
  (
    x0 Nullable(Int32),
    x1 Nullable(Int32),
    x2 Int32
  ) ENGINE = MergeTree ORDER BY tuple();
"

echo "Unless specified otherwise, operations use:"
echo $SET_INPUT
echo $SET_OUTPUT

echo
echo "Insert $INITIAL_INSERT_VALUES into table (Nullable(String), Int32):"
$CLICKHOUSE_CLIENT -n --query "
  INSERT INTO $MAIN_TABLE VALUES $INITIAL_INSERT_VALUES;
  SELECT * FROM $MAIN_TABLE;
"

echo
echo "Protobuf representation of the second row:"
$CLICKHOUSE_CLIENT -n --query "$SET_OUTPUT SELECT * FROM $MAIN_TABLE WHERE ref = 2 LIMIT 1 $(protobuf_info output ProtobufSingle Message)" > "$BINARY_FILE_PATH"
hexdump -C $BINARY_FILE_PATH

echo
echo "Decoded with protoc:"
(cd $SCHEMADIR && $PROTOC_BINARY --decode Message "$PROTOBUF_FILE_NAME".proto) < $BINARY_FILE_PATH

echo
echo "Proto message with wrapper for (NULL, 1), ('', 2), ('str', 3):"
printf '\x02\x10\x01' > "$MESSAGE_FILE_PATH" # (NULL, 1)
printf '\x04\x0A\x00\x10\x02' >> "$MESSAGE_FILE_PATH" # ('', 2)
printf '\x09\x0A\x05\x0A\x03\x73\x74\x72\x10\x03' >> "$MESSAGE_FILE_PATH" # ('str', 3)
hexdump -C $MESSAGE_FILE_PATH

echo
echo "Insert proto message into table (Nullable(String), Int32):"
$CLICKHOUSE_CLIENT -n --query "$SET_INPUT INSERT INTO $ROUNDTRIP_TABLE $(protobuf_info input Protobuf Message)" < "$MESSAGE_FILE_PATH"
$CLICKHOUSE_CLIENT --query "SELECT * FROM $ROUNDTRIP_TABLE"

echo
echo "Proto output of the table using Google wrapper:"
$CLICKHOUSE_CLIENT -n --query "$SET_OUTPUT SELECT * FROM $ROUNDTRIP_TABLE $(protobuf_info output Protobuf Message)" > "$BINARY_FILE_PATH"
hexdump -C $BINARY_FILE_PATH

echo
echo "Proto output of the table without Google wrapper:"
$CLICKHOUSE_CLIENT --query "SELECT * FROM $ROUNDTRIP_TABLE $(protobuf_info output Protobuf MessageNoWrapper)" > "$BINARY_FILE_PATH"
hexdump -C $BINARY_FILE_PATH

echo
echo "Insert proto message into table (Tuple(String), Int32)"
echo "with disabled Google wrappers flattening:"
$CLICKHOUSE_CLIENT --query "INSERT INTO $COMPATIBILITY_TABLE $(protobuf_info input Protobuf Message)" < "$MESSAGE_FILE_PATH"
$CLICKHOUSE_CLIENT --query "SELECT * FROM $COMPATIBILITY_TABLE"

echo
echo "Insert $MULTI_WRAPPER_VALUES and reinsert using Google wrappers into:"
echo "Table (Nullable(Int32), Nullable(Int32), Int32):"
$CLICKHOUSE_CLIENT --query "INSERT INTO $MULTI_TABLE VALUES $MULTI_WRAPPER_VALUES"
$CLICKHOUSE_CLIENT -n --query "$SET_OUTPUT SELECT * FROM $MULTI_TABLE $(protobuf_info output Protobuf MessageMultiWrapper)" > "$BINARY_FILE_PATH"
$CLICKHOUSE_CLIENT -n --query "$SET_INPUT INSERT INTO $MULTI_TABLE $(protobuf_info input Protobuf MessageMultiWrapper)" < "$BINARY_FILE_PATH"
$CLICKHOUSE_CLIENT --query "SELECT * FROM $MULTI_TABLE"

rm "$BINARY_FILE_PATH"
rm "$MESSAGE_FILE_PATH"

$CLICKHOUSE_CLIENT -n --query "
  DROP TABLE $MAIN_TABLE;
  DROP TABLE $ROUNDTRIP_TABLE;
  DROP TABLE $COMPATIBILITY_TABLE;
  DROP TABLE $MULTI_TABLE;
"
