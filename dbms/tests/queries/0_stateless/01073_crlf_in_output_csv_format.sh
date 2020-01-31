#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_01073_crlf_in_output_csv_format;"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_01073_crlf_in_output_csv_format (value UInt8, word String) ENGINE = MergeTree() ORDER BY value;"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test_01073_crlf_in_output_csv_format VALUES (1, 'hello'), (2, 'world');"

if [[ `${CLICKHOUSE_CLIENT} --output_format_csv_crlf_end_of_line=0 --query "SELECT * FROM test_01073_crlf_in_output_csv_format FORMAT CSV;"`="1,\"hello\"\n2,\"world\"\n" ]]; then
    echo "OK"
else
    echo "WA"
fi

if [[ `${CLICKHOUSE_CLIENT} --output_format_csv_crlf_end_of_line=1 --query "SELECT * FROM test_01073_crlf_in_output_csv_format FORMAT CSV;"`="1,\"hello\"\r\n2,\"world\"\r\n" ]]; then
    echo "OK"
else
    echo "WA"
fi

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_01073_crlf_in_output_csv_format;"

