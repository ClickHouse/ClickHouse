#!/usr/bin/env bash
clickhouse-client -n --query "DROP TABLE IF EXISTS test.json_noisy; CREATE TABLE test.json_noisy (d1 UInt8, d2 String) ENGINE = Memory"

echo '{"d1" : 1, "d2" : "ok"}
{ }
{"t1" : 0, "t2":true,"t3":false, "t4":null,"t5":[],"t6":"trash" }
{"d2":"ok","t1":[[[]],true, null, false, "1","2",9.03,101], "t2":[["1","2"]], "d1":"1"}
{"d2":"ok","t1":[[[]],true, null, false, "1","2", 0.03, 1], "d1":"1", "t2":["1","2"]}' \
| clickhouse-client -n --query "SET input_format_skip_unknown_fields = 1; INSERT INTO test.json_noisy FORMAT JSONEachRow"

clickhouse-client -n --query "SELECT * FROM test.json_noisy; DROP TABLE IF EXISTS test.json_noisy;"
