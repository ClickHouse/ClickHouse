#!/usr/bin/env bash
set -e

clickhouse-client -q "DROP TABLE IF EXISTS test.json_noisy"
clickhouse-client -q "CREATE TABLE test.json_noisy (d1 UInt8, d2 String) ENGINE = Memory"

echo '{"d1" : 1, "d2" : "ok"}
{ }
{"t1" : 0, "t2":true,"t3":false, "t4":null,"t5":[],"t6":"trash" }
{"d2":"ok","t1":[[[]],true, null, false, "1","2",9.03,101], "t2":[["1","2"]], "d1":"1"}
{"d2":"ok","t1":[[[]],true, null, false, "1","2", 0.03, 1], "d1":"1", "t2":["1","2"]}
{"t0" : -0.1, "t1" : +1, "t2" : 0, "t3" : [0.0, -0.1], "d2" : "ok", "d1" : 1}' \
| clickhouse-client --input_format_skip_unknown_fields=1 -q "INSERT INTO test.json_noisy FORMAT JSONEachRow"

clickhouse-client --max_threads=1 -q "SELECT * FROM test.json_noisy"
clickhouse-client -q "DROP TABLE IF EXISTS test.json_noisy"
