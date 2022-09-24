#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT -q "select toFloat64(50.36) as fl, toString('hello') as str, toDateTime64(247000000000000000, 3) as date, toInt32(42) as num1, toInt64(43) as num2, toUInt64(44) as num3, [toInt32(42), toInt32(13)] as arr, Null as nothing, True as bool, toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') as id format BSONEachRow" > /home/ubuntu/ClickHouse/programs/server/user_files/test.bson
$CLICKHOUSE_CLIENT -q "select * from file(test.bson, BSONEachRow, 'fl Float64, str String, date DateTime64, num1 Int32, num2 Int64, num3 UInt64, arr Array(Int32), nothing Nullable(Nothing), bool UInt8, id UUID') format JSONEachRow"
