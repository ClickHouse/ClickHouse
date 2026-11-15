#!/usr/bin/env bash
# Tags: no-fasttest

# Addresses https://github.com/ClickHouse/ClickHouse/issues/90669

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCHEMADIR=$CURDIR/format_schemas
. "$CURDIR"/../shell_config.sh

rm -rf "${CLICKHOUSE_TMP}"/oneof_several_values.bin

# generate a Protobuf file that has values for both string1 and string2
# despite the fact that these two strings belong to OneOf and cannot coexist
${CLICKHOUSE_LOCAL} --logger.console=0 --query "
CREATE TABLE string_or_string
(
string1 String,
string2 String
)
ENGINE = MergeTree
ORDER BY tuple();
insert into string_or_string values ('str1',''), ('','str2');
SELECT * FROM string_or_string
SETTINGS format_schema_source='string',
format_schema = 'syntax = \"proto3\";message StringOrString {oneof string_oneof {string string1 = 1;string string2 = 42;}}',
format_schema_message_name='StringOrString'
FORMAT Protobuf;
" > "${CLICKHOUSE_TMP}/oneof_several_values.bin"

# successfully read from this Protobuf file
${CLICKHOUSE_LOCAL} --logger.console=0 --input-format=Protobuf --structure="string1 String, string2 String" --query "
SELECT *
FROM file('stdin', Protobuf)
SETTINGS format_schema_source='string',
format_schema = 'syntax = \"proto3\";message StringOrString {oneof string_oneof {string string1 = 1;string string2 = 42;}}',
format_schema_message_name='StringOrString';
" < "${CLICKHOUSE_TMP}/oneof_several_values.bin"

# raise error reading from the file if `input_format_protobuf_oneof_presence` is set
${CLICKHOUSE_LOCAL} --logger.console=0 --input-format=Protobuf --structure="string1 String, string2 String" --query "
SELECT *
FROM file('stdin', Protobuf)
SETTINGS format_schema_source='string',
format_schema = 'syntax = \"proto3\";message StringOrString {oneof string_oneof {string string1 = 1;string string2 = 42;}}',
format_schema_message_name='StringOrString', input_format_protobuf_oneof_presence = true; -- { serverError BAD_ARGUMENTS }
" < "${CLICKHOUSE_TMP}/oneof_several_values.bin"

rm -rf "${CLICKHOUSE_TMP}"/oneof_several_values.bin
