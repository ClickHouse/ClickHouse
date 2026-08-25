#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo 'syntax = "proto3";

message Message {
    NotExisted x = 1;
}' > 02705_schema_$CLICKHOUSE_TEST_UNIQUE_NAME.proto


$CLICKHOUSE_LOCAL -q "select * from file(data.bin, Protobuf) settings format_schema='02705_schema_$CLICKHOUSE_TEST_UNIQUE_NAME:Message'" 2>&1 | grep -c "CANNOT_PARSE_PROTOBUF_SCHEMA"

rm 02705_schema_$CLICKHOUSE_TEST_UNIQUE_NAME.proto

