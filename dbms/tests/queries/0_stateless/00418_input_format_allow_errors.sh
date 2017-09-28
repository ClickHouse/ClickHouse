#!/usr/bin/env bash

clickhouse-client --query="CREATE DATABASE IF NOT EXISTS test"
clickhouse-client --query="DROP TABLE IF EXISTS test.formats_test"
clickhouse-client --query="CREATE TABLE test.formats_test (x UInt64, s String) ENGINE = Memory"

echo -ne '1\tHello\n \n3\tGoodbye\n' | clickhouse-client --input_format_allow_errors_num=1 --input_format_allow_errors_ratio=0.1 --query="INSERT INTO test.formats_test FORMAT TSV"

clickhouse-client --query="SELECT * FROM test.formats_test ORDER BY x, s"

echo -ne '1\tHello\n2\n3\tGoodbye\n\n' | clickhouse-client --input_format_allow_errors_num=1 --input_format_allow_errors_ratio=0.1 --query="INSERT INTO test.formats_test FORMAT TSV" 2> /dev/null; echo $?

echo -ne '1\tHello\n2\n3\tGoodbye\n\n' | clickhouse-client --input_format_allow_errors_num=2 --input_format_allow_errors_ratio=0.1 --query="INSERT INTO test.formats_test FORMAT TSV"

clickhouse-client --query="SELECT * FROM test.formats_test ORDER BY x, s"

echo -ne '1\tHello\n2\n3\tGoodbye\n\n' | clickhouse-client --input_format_allow_errors_num=1 --input_format_allow_errors_ratio=0.4 --query="INSERT INTO test.formats_test FORMAT TSV" 2> /dev/null; echo $?

echo -ne '1\tHello\n2\n3\tGoodbye\n\n' | clickhouse-client --input_format_allow_errors_num=1 --input_format_allow_errors_ratio=0.6 --query="INSERT INTO test.formats_test FORMAT TSV"

echo -ne 'x=1\ts=TSKV\nx=minus2\ts=trash1\ns=trash2\tx=-3\ns=TSKV Ok\tx=4\ns=trash3\tx=-5\n' | clickhouse-client --input_format_allow_errors_num=3 -q "INSERT INTO test.formats_test FORMAT TSKV"

clickhouse-client --query="SELECT * FROM test.formats_test ORDER BY x, s"

clickhouse-client --query="DROP TABLE test.formats_test"
