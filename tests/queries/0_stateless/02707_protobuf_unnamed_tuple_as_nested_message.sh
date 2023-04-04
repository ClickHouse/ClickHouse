#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo 'syntax = "proto3";

message Nested {
    int32 a = 1;
    string b = 2;
    repeated int32 c = 3;
};

message Message {
    Nested x = 1;
};' > 02707_schema_$CLICKHOUSE_TEST_UNIQUE_NAME.proto


$CLICKHOUSE_LOCAL -q "select tuple(42, 'Hello', [1,2,3]) as x format Protobuf settings format_schema='02707_schema_$CLICKHOUSE_TEST_UNIQUE_NAME:Message'" | $CLICKHOUSE_LOCAL --input-format Protobuf --structure='x Tuple(UInt32, String, Array(UInt32))' -q "select * from table" --format_schema="02707_schema_$CLICKHOUSE_TEST_UNIQUE_NAME:Message"

rm 02707_schema_$CLICKHOUSE_TEST_UNIQUE_NAME.proto

