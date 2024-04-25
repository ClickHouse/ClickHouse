#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS cbor";

$CLICKHOUSE_CLIENT --query="CREATE TABLE cbor (uint8 UInt8, uint16 UInt16, uint32 UInt32, uint64 UInt64, int8 Int8, int16 Int16, int32 Int32, int64 Int64, float Float32, double Float64, string String, date Date) ENGINE = Memory";
$CLICKHOUSE_CLIENT --query="INSERT INTO cbor VALUES (255, 65535, 4294967295, 100000000000, -128, -32768, -2147483648, -100000000000, 2.02, 10000.0000001, 'String', 18980), (4, 1234, 3244467295, 500000000000, -1, -256, -14741221, -7000000000, 100.1, 14321.032141201, 'Another string', 20000),(42, 42, 42, 42, 42, 42, 42, 42, 42.42, 42.42, '42', 42)";
$CLICKHOUSE_CLIENT --query="SELECT * FROM cbor FORMAT CBOR";
$CLICKHOUSE_CLIENT --query="DROP TABLE cbor";