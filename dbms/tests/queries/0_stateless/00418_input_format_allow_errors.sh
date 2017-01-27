#!/usr/bin/env bash

clickhouse-client --query="DROP TABLE IF EXISTS test.formats_test"
clickhouse-client --query="CREATE TABLE test.formats_test (x UInt64, s String) ENGINE = Memory"

echo -ne '1\tHello\n \n3\tGoodbye\n' | clickhouse-client --input_format_allow_errors_num=1 --input_format_allow_errors_ratio=0.1 --query="INSERT INTO test.formats_test FORMAT TSV"

clickhouse-client --query="SELECT * FROM test.formats_test"

echo -ne '1\tHello\n2\n3\tGoodbye\n\n' | clickhouse-client --input_format_allow_errors_num=1 --input_format_allow_errors_ratio=0.1 --query="INSERT INTO test.formats_test FORMAT TSV" 2> /dev/null; echo $?

echo -ne '1\tHello\n2\n3\tGoodbye\n\n' | clickhouse-client --input_format_allow_errors_num=2 --input_format_allow_errors_ratio=0.1 --query="INSERT INTO test.formats_test FORMAT TSV"

clickhouse-client --query="SELECT * FROM test.formats_test"

echo -ne '1\tHello\n2\n3\tGoodbye\n\n' | clickhouse-client --input_format_allow_errors_num=1 --input_format_allow_errors_ratio=0.4 --query="INSERT INTO test.formats_test FORMAT TSV" 2> /dev/null; echo $?

echo -ne '1\tHello\n2\n3\tGoodbye\n\n' | clickhouse-client --input_format_allow_errors_num=1 --input_format_allow_errors_ratio=0.6 --query="INSERT INTO test.formats_test FORMAT TSV"

clickhouse-client --query="SELECT * FROM test.formats_test"

clickhouse-client --query="DROP TABLE test.formats_test"
