#!/usr/bin/env bash
# Tags: no-fasttest

# A Protobuf string field converted to `DateTime`/`DateTime64` is a complete value, so nothing may be
# left unread after parsing it. Before the fix `2024 April 4` was silently truncated to the unix
# timestamp `2024` (`1970-01-01 00:33:44`) instead of being rejected.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCHEMADIR=$CUR_DIR/format_schemas
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SCHEMA="$SCHEMADIR/04656_protobuf_datetime_reject_trailing.proto:Row"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS protobuf_datetime_trailing"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS protobuf_datetime64_trailing"
$CLICKHOUSE_CLIENT --query "CREATE TABLE protobuf_datetime_trailing (ts DateTime('UTC')) ENGINE = MergeTree ORDER BY ts"
$CLICKHOUSE_CLIENT --query "CREATE TABLE protobuf_datetime64_trailing (ts DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY ts"

# Length-delimited Protobuf messages for `message Row { string ts = 1; }`: the message length,
# then the tag of field 1 with wire type LENGTH_DELIMITED (0x0a), then the string length and bytes.
printf '\x15\x0a\x132024-01-15 10:11:12' \
    | $CLICKHOUSE_CLIENT --query "INSERT INTO protobuf_datetime_trailing SETTINGS format_schema = '$SCHEMA' FORMAT Protobuf"
# Unlike the `DateTime` serializer, which uses the time zone of the column, the `DateTime64` one
# parses the text in the session time zone (`readDateTime64Text` without a `DateLUTImpl` in
# `ProtobufSerializerDecimal`), so pin it to keep the test independent of the randomized setting.
printf '\x19\x0a\x172024-01-15 10:11:12.500' \
    | $CLICKHOUSE_CLIENT --session_timezone UTC \
        --query "INSERT INTO protobuf_datetime64_trailing SETTINGS session_timezone = 'UTC', format_schema = '$SCHEMA' FORMAT Protobuf"

$CLICKHOUSE_CLIENT --query "SELECT toString(ts, 'UTC') FROM protobuf_datetime_trailing"
$CLICKHOUSE_CLIENT --query "SELECT toString(ts, 'UTC') FROM protobuf_datetime64_trailing"

printf '\x0e\x0a\x0c2024 April 4' \
    | $CLICKHOUSE_CLIENT --input_format_allow_errors_num 0 --input_format_allow_errors_ratio 0 \
        --query "INSERT INTO protobuf_datetime_trailing SETTINGS format_schema = '$SCHEMA' FORMAT Protobuf" 2>&1 \
    | grep -c -F -e "CANNOT_PARSE_INPUT_ASSERTION_FAILED" -e "CANNOT_PARSE_DATETIME"
printf '\x0e\x0a\x0c2024 April 4' \
    | $CLICKHOUSE_CLIENT --input_format_allow_errors_num 0 --input_format_allow_errors_ratio 0 \
        --query "INSERT INTO protobuf_datetime64_trailing SETTINGS format_schema = '$SCHEMA' FORMAT Protobuf" 2>&1 \
    | grep -c -F -e "CANNOT_PARSE_INPUT_ASSERTION_FAILED" -e "CANNOT_PARSE_DATETIME"

# The rejected rows were not inserted.
$CLICKHOUSE_CLIENT --query "SELECT count() FROM protobuf_datetime_trailing"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM protobuf_datetime64_trailing"

$CLICKHOUSE_CLIENT --query "DROP TABLE protobuf_datetime_trailing"
$CLICKHOUSE_CLIENT --query "DROP TABLE protobuf_datetime64_trailing"
