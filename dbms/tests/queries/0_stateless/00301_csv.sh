#!/usr/bin/env bash

clickhouse-client --query="DROP TABLE IF EXISTS test.csv";
clickhouse-client --query="CREATE TABLE test.csv (s String, n UInt64, d Date) ENGINE = Memory";

echo '"Hello, world", 123, "2016-01-01"
"Hello, ""world""", "456", 2016-01-02,
Hello "world", 789 ,2016-01-03
"Hello
 world", 100, 2016-01-04,' | clickhouse-client --query="INSERT INTO test.csv FORMAT CSV";

clickhouse-client --query="SELECT * FROM test.csv ORDER BY d";
clickhouse-client --query="DROP TABLE test.csv";
