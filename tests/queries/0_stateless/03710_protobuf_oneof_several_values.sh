#!/usr/bin/env bash
# Tags: no-fasttest

# Addresses https://github.com/ClickHouse/ClickHouse/issues/90669

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCHEMADIR=$CURDIR/format_schemas
. "$CURDIR"/../shell_config.sh

set -e

[ -e "${CLICKHOUSE_TMP}"/oneof_several_values.bin ] && rm "${CLICKHOUSE_TMP}"/oneof_several_values.bin


$CLICKHOUSE_CLIENT <<EOF
CREATE TABLE string_or_string
(
string1 String,
string2 String
)
ENGINE = MergeTree
ORDER BY tuple();

insert into string_or_string values ('str1',''), ('','str2');

SELECT * FROM string_or_string
INTO OUTFILE '${CLICKHOUSE_TMP}/oneof_several_values.bin'
SETTINGS format_schema_source='string',
format_schema = 'syntax = "proto3";message StringOrString {oneof string_oneof {string string1 = 1;string string2 = 42;}}',
format_schema_message_name='StringOrString'
FORMAT Protobuf;

SELECT * FROM file('${CLICKHOUSE_TMP}/oneof_several_values.bin', Protobuf)
SETTINGS format_schema_source='string',
format_schema = 'syntax = "proto3";message StringOrString {oneof string_oneof {string string1 = 1;string string2 = 42;}}',
format_schema_message_name='StringOrString';
EOF

$CLICKHOUSE_CLIENT <<EOF
SELECT *
FROM file('${CLICKHOUSE_TMP}/oneof_several_values.bin', Protobuf)
SETTINGS format_schema_source='string',
format_schema = 'syntax = "proto3";message StringOrString {oneof string_oneof {string string1 = 1;string string2 = 42;}}',
format_schema_message_name='StringOrString', input_format_protobuf_oneof_presence = true; -- { serverError PROTOBUF_ONEOF_HAS_SEVERAL_VALUES }
EOF

rm "${CLICKHOUSE_TMP}"/oneof_several_values.bin
