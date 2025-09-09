#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q 'CREATE FUNCTION test_driver RETURNS Int32 ENGINE = TestPython AS $$print(123)$$'
$CLICKHOUSE_LOCAL -q 'SELECT test_driver()'

$CLICKHOUSE_LOCAL -q 'CREATE FUNCTION test_driver RETURNS Int32 ENGINE = TestPython AS $$print(123)$$ -- { serverError FUNCTION_ALREADY_EXISTS }'

$CLICKHOUSE_LOCAL -q 'CREATE FUNCTION IF NOT EXISTS test_driver RETURNS Int32 ENGINE = TestPython AS $$print(5)$$'
$CLICKHOUSE_LOCAL -q 'SELECT test_driver()'

$CLICKHOUSE_LOCAL -q 'CREATE OR REPLACE FUNCTION test_driver RETURNS Int32 ENGINE = TestPython AS $$print(5)$$'
$CLICKHOUSE_LOCAL -q 'SELECT test_driver()'

$CLICKHOUSE_LOCAL -q 'DROP FUNCTION test_driver'
$CLICKHOUSE_LOCAL -q 'SELECT test_driver() -- { serverError UNKNOWN_FUNCTION }'

$CLICKHOUSE_LOCAL -q 'CREATE FUNCTION IF NOT EXISTS test_arg_driver(Col1 Int32, Col2 Int32) RETURNS String ENGINE = TestPython AS $$
    import sys

    for line in sys.stdin:
        print(line.replace("\t", "-"), end="")
        sys.stdout.flush()
$$'
$CLICKHOUSE_LOCAL -q 'SELECT test_arg_driver(1, 2)'

$CLICKHOUSE_LOCAL -q 'CREATE OR REPLACE FUNCTION test_arg_driver(Col1 Int32, Col2 Int32) RETURNS Int64 ENGINE = TestPython AS $$
    import sys

    for line in sys.stdin:
        col1, col2 = map(int, line.split("\t"))
        print(col1 ** 2 + col2 ** 2)
        sys.stdout.flush()
$$'
$CLICKHOUSE_LOCAL -q 'SELECT test_arg_driver(4, 5)'

$CLICKHOUSE_LOCAL -q 'CREATE TABLE IF NOT EXISTS test_driver_table(Col1 Int32 PRIMARY KEY, Col2 Int32)'
$CLICKHOUSE_LOCAL -q 'INSERT INTO test_driver_table (Col1, Col2) VALUES (1, 2), (3, 5), (-4, 0), (5, -8)'
$CLICKHOUSE_LOCAL -q 'SELECT test_arg_driver(Col1, Col2) FROM test_driver_table'

$CLICKHOUSE_LOCAL -q 'DROP FUNCTION IF EXISTS test_arg_driver'
$CLICKHOUSE_LOCAL -q 'DROP FUNCTION test_arg_driver -- { serverError UNKNOWN_FUNCTION }'

$CLICKHOUSE_LOCAL -q 'CREATE FUNCTION IF NOT EXISTS test_driver RETURNS Int32 ENGINE = Nothing AS $$echo 1$$ -- { serverError UNSUPPORTED_DRIVER }'
