#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS csv";

$CLICKHOUSE_CLIENT --query="CREATE TABLE csv (s String, n UInt64, d Date) ENGINE = Memory";

echo "'single quote' not end, 123, 2016-01-01
'em good, 456, 2016-01-02" | $CLICKHOUSE_CLIENT --format_csv_allow_single_quotes=0 --query="INSERT INTO csv FORMAT CSV";

$CLICKHOUSE_CLIENT --query="SELECT * FROM csv ORDER BY d";

$CLICKHOUSE_CLIENT --query="DROP TABLE csv";

$CLICKHOUSE_CLIENT --query="CREATE TABLE csv (s String, n UInt64, d Date) ENGINE = Memory";

echo "'single quote' not end, 123, 2016-01-01
'em good, 456, 2016-01-02" | $CLICKHOUSE_CLIENT --multiquery --query="SET format_csv_allow_single_quotes=0; INSERT INTO csv FORMAT CSV";

$CLICKHOUSE_CLIENT --query="SELECT * FROM csv ORDER BY d";

$CLICKHOUSE_CLIENT --query="DROP TABLE csv";

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS csv";

$CLICKHOUSE_CLIENT --query="CREATE TABLE csv (s String, n UInt64, d Date) ENGINE = Memory";

echo '"double quote" not end, 123, 2016-01-01
"em good, 456, 2016-01-02' | $CLICKHOUSE_CLIENT --format_csv_allow_double_quotes=0 --query="INSERT INTO csv FORMAT CSV";

$CLICKHOUSE_CLIENT --query="SELECT * FROM csv ORDER BY d";

$CLICKHOUSE_CLIENT --query="DROP TABLE csv";

$CLICKHOUSE_CLIENT --query="CREATE TABLE csv (s String, n UInt64, d Date) ENGINE = Memory";

echo '"double quote" not end, 123, 2016-01-01
"em good, 456, 2016-01-02' | $CLICKHOUSE_CLIENT --multiquery --query="SET format_csv_allow_double_quotes=0; INSERT INTO csv FORMAT CSV";

$CLICKHOUSE_CLIENT --query="SELECT * FROM csv ORDER BY d";

$CLICKHOUSE_CLIENT --query="DROP TABLE csv";
