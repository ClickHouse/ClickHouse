#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

echo "
    DROP TABLE IF EXISTS test.test_mutations_with_ast_elements;
    SET max_ast_elements = 1000;
    CREATE TABLE test.test_mutations_with_ast_elements(date Date, a UInt64, b String) ENGINE = MergeTree(date, (a, date), 8192);
    INSERT INTO test.test_mutations_with_ast_elements SELECT '2019-07-29' AS date, 1, toString(number) FROM numbers(100000);
    SYSTEM STOP MERGES test.test_mutations_with_ast_elements;
" | $CLICKHOUSE_CLIENT -n

for i in {1..10000}; do echo "ALTER TABLE test.test_mutations_with_ast_elements DELETE WHERE toUInt32(b) = ${i};" | $CLICKHOUSE_CLIENT -n ; done;

echo "
    SYSTEM START MERGES test.test_mutations_with_ast_elements;
    OPTIMIZE TABLE test.test_mutations_with_ast_elements FINAL;
    DROP TABLE IF EXISTS test.test_mutations_with_ast_elements;
" | $CLICKHOUSE_CLIENT -n

