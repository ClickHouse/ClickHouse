#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test.json_each_row_nested"
$CLICKHOUSE_CLIENT -q "CREATE TABLE test.json_each_row_nested (d1 UInt8, d2 String, n Nested (s String, i Int32) ) ENGINE = Memory"

echo '{"d1" : 1, "d2" : "ok", "n.s" : ["abc", "def"], "n.i" : [1, 23]}
{ }
{"t1" : 0, "n.t2":true,"n.i":[45, 67, 8], "t4":null,"t5":[],"t6":"trash" }
{"d2":"ok","n.s":["dog", "cat", "pig"], "n.x":[["1","2"]], "d1":"1", "n.i":[3, 3, 3]}
{"t0" : -0.1, "n.s" : ["zero","negative one"], "a.b" : 0, "n.i" : [0, -1], "d2" : "ok", "d1" : 1}' \
| $CLICKHOUSE_CLIENT --input_format_skip_unknown_fields=1 -q "INSERT INTO test.json_each_row_nested FORMAT JSONEachRow"

$CLICKHOUSE_CLIENT --max_threads=1 -q "SELECT * FROM test.json_each_row_nested"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test.json_each_row_nested"

$CLICKHOUSE_CLIENT -q "CREATE TABLE test.json_each_row_nested (d1 UInt8, d2 String, n Nested (s String, i Int32) ) ENGINE = Memory"

echo '{"d1" : 1, "d2" : "ok", "n" : { "s" : ["abc", "def"], "i" : [1, 23]} }
{ }
{"t1" : 0, "n.t2":true,"n" : {"i":[45, 67, 8]}, "t4":null,"t5":[],"t6":"trash" }
{"d2":"ok","n" : {"s":["dog", "cat", "pig"], "x":[["1","2"]]}, "d1":"1", "n.i":[3, 3, 3]}
{"t0" : -0.1, "n": {"s" : ["zero","negative one"], "i" : [0, -1]}, "d2" : "ok", "d1" : 1}' \
| $CLICKHOUSE_CLIENT --input_format_skip_unknown_fields=1 -q "INSERT INTO test.json_each_row_nested FORMAT JSONEachRow"

$CLICKHOUSE_CLIENT --max_threads=1 -q "SELECT * FROM test.json_each_row_nested"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test.json_each_row_nested"
