#!/usr/bin/env bash

clickhouse-client -q "CREATE DATABASE IF NOT EXISTS test";
clickhouse-client -q "DROP TABLE IF EXISTS test.ws";
clickhouse-client -q "CREATE TABLE test.ws (i UInt8) ENGINE = Memory";

clickhouse-client -q "INSERT INTO test.ws FORMAT RowBinary ;";
clickhouse-client -q "INSERT INTO test.ws FORMAT RowBinary 	; ";
clickhouse-client -q "INSERT INTO test.ws FORMAT RowBinary
; ";
echo -n ";" | clickhouse-client -q "INSERT INTO test.ws FORMAT RowBinary";

clickhouse-client --max_threads=1 -q "SELECT * FROM test.ws";
clickhouse-client -q "DROP TABLE test.ws";


clickhouse-client -q "SELECT ''";


clickhouse-client -q "CREATE TABLE test.ws (s String) ENGINE = Memory";
clickhouse-client -q "INSERT INTO test.ws FORMAT TSV	;
";
echo ";" | clickhouse-client -q "INSERT INTO test.ws FORMAT TSV"
if clickhouse-client -q "INSERT INTO test.ws FORMAT TSV;" 1>/dev/null 2>/dev/null; then
    echo ERROR;
fi
clickhouse-client --max_threads=1 -q "SELECT * FROM test.ws";

clickhouse-client -q "DROP TABLE test.ws";
