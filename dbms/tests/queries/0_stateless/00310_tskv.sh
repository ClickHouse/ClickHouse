#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

clickhouse-client --query="DROP TABLE IF EXISTS test.tskv";
clickhouse-client --query="CREATE TABLE test.tskv (tskv_format String, timestamp DateTime, timezone String, text String, binary_data String) ENGINE = Memory";

echo -n 'tskv	tskv_format=custom-service-log	timestamp=2013-01-01 00:00:00	timezone=+0400	text=multiline\ntext	binary_data=can contain \0 symbol
binary_data=abc	text=Hello, world
binary_data=def	text=
tskv

' | clickhouse-client --query="INSERT INTO test.tskv FORMAT TSKV";

clickhouse-client --query="SELECT * FROM test.tskv ORDER BY binary_data";
clickhouse-client --query="DROP TABLE test.tskv";
