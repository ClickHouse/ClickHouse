#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

clickhouse-client --query="SELECT 1"
clickhouse-client --query="SELECT 1;"
clickhouse-client --query="SELECT 1; "
clickhouse-client --query="SELECT 1 ; "

clickhouse-client --query="SELECT 1; S" 2>&1 | grep -o 'Syntax error'
clickhouse-client --query="SELECT 1; SELECT 2" 2>&1 | grep -o 'Syntax error'
clickhouse-client --query="SELECT 1; SELECT 2;" 2>&1 | grep -o 'Syntax error'
clickhouse-client --query="SELECT 1; SELECT 2; SELECT" 2>&1 | grep -o 'Syntax error'

clickhouse-client -n --query="SELECT 1; S" 2>&1 | grep -o 'Syntax error'
clickhouse-client -n --query="SELECT 1; SELECT 2"
clickhouse-client -n --query="SELECT 1; SELECT 2;"
clickhouse-client -n --query="SELECT 1; SELECT 2; SELECT" 2>&1 | grep -o 'Syntax error'

clickhouse-client -n --query="DROP TABLE IF EXISTS test.t; CREATE TABLE test.t (x UInt64) ENGINE = TinyLog;"

clickhouse-client --query="INSERT INTO test.t VALUES (1),(2),(3);"
clickhouse-client --query="SELECT * FROM test.t"
clickhouse-client --query="INSERT INTO test.t VALUES" <<< "(4),(5),(6)"
clickhouse-client --query="SELECT * FROM test.t"

clickhouse-client -n --query="INSERT INTO test.t VALUES (1),(2),(3);"
clickhouse-client -n --query="SELECT * FROM test.t"
clickhouse-client -n --query="INSERT INTO test.t VALUES" <<< "(4),(5),(6)"
clickhouse-client -n --query="SELECT * FROM test.t"

curl -sS 'http://localhost:8123/' -d "SELECT 1"
curl -sS 'http://localhost:8123/' -d "SELECT 1;"
curl -sS 'http://localhost:8123/' -d "SELECT 1; "
curl -sS 'http://localhost:8123/' -d "SELECT 1 ; "

curl -sS 'http://localhost:8123/' -d "SELECT 1; S" 2>&1 | grep -o 'Syntax error'
curl -sS 'http://localhost:8123/' -d "SELECT 1; SELECT 2" 2>&1 | grep -o 'Syntax error'
curl -sS 'http://localhost:8123/' -d "SELECT 1; SELECT 2;" 2>&1 | grep -o 'Syntax error'
curl -sS 'http://localhost:8123/' -d "SELECT 1; SELECT 2; SELECT" 2>&1 | grep -o 'Syntax error'

curl -sS 'http://localhost:8123/' -d "INSERT INTO test.t VALUES (1),(2),(3);"
clickhouse-client --query="SELECT * FROM test.t"
curl -sS 'http://localhost:8123/?query=INSERT' -d "INTO test.t VALUES (4),(5),(6);"
clickhouse-client --query="SELECT * FROM test.t"
curl -sS 'http://localhost:8123/?query=INSERT+INTO+test.t+VALUES' -d "(7),(8),(9)"
clickhouse-client --query="SELECT * FROM test.t"

clickhouse-client -n --query="DROP TABLE test.t;"
