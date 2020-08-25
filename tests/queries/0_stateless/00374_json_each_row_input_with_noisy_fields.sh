#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS json_noisy"
$CLICKHOUSE_CLIENT -q "CREATE TABLE json_noisy (d1 UInt8, d2 String) ENGINE = Memory"

echo '{"d1" : 1, "d2" : "ok"}
{ }
{"t1" : 0, "t2":true,"t3":false, "t4":null,"t5":[],"t6":"trash" }
{"d2":"ok","t1":[[[]],true, null, false, "1","2",9.03,101], "t2":[["1","2"]], "d1":"1"}
{"d2":"ok","t1":[[[]],true, null, false, "1","2", 0.03, 1], "d1":"1", "t2":["1","2"]}
{"d2":"ok","t1":{"a":{"b": {} ,"c":false},"b":[true,null, false]}, "t2":  { "a": [  ] } , "d1":1}
{"t0" : -0.1, "t1" : +1, "t2" : 0, "t3" : [0.0, -0.1], "d2" : "ok", "d1" : 1}' \
| $CLICKHOUSE_CLIENT --input_format_skip_unknown_fields=1 -q "INSERT INTO json_noisy FORMAT JSONEachRow"

$CLICKHOUSE_CLIENT --max_threads=1 -q "SELECT * FROM json_noisy"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS json_noisy"

# Regular test for DateTime

echo
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS json_each_row"
$CLICKHOUSE_CLIENT -q "CREATE TABLE json_each_row (d DateTime('Europe/Moscow')) ENGINE = Memory"
echo '{"d" : "2017-08-31 18:36:48", "t" : ""}
{"d" : "1504193808", "t" : -1}
{"d" : 1504193808, "t" : []}
{"d" : 01504193808, "t" : []}' \
| $CLICKHOUSE_CLIENT --input_format_skip_unknown_fields=1 -q "INSERT INTO json_each_row FORMAT JSONEachRow"
$CLICKHOUSE_CLIENT -q "SELECT DISTINCT * FROM json_each_row"
$CLICKHOUSE_CLIENT -q "DROP TABLE json_each_row"
