#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

EXCEPTION_SUCCESS_TEXT=ok
$CLICKHOUSE_CLIENT --query="CREATE DATABASE IF NOT EXISTS test;"
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS test_constraints;"

$CLICKHOUSE_CLIENT --query="CREATE TABLE test_constraints
(
        a       UInt32,
        b       UInt32,
        CONSTRAINT b_constraint CHECK b > 0
)
ENGINE = MergeTree ORDER BY (a);"

# This one must succeed
$CLICKHOUSE_CLIENT --query="INSERT INTO test_constraints VALUES (1, 2);"
$CLICKHOUSE_CLIENT --query="SELECT * FROM test_constraints;"

# This one must throw and exception
EXCEPTION_TEXT="Violated constraint b_constraint in table test_constraints at indices {1, 3}"
$CLICKHOUSE_CLIENT --query="INSERT INTO test_constraints VALUES (3, 4), (1, 0), (3, 4), (6, 0);" 2>&1 \
    | grep -q "$EXCEPTION_TEXT" && echo "$EXCEPTION_SUCCESS_TEXT" || echo "Did not thrown an exception"
$CLICKHOUSE_CLIENT --query="SELECT * FROM test_constraints;"

$CLICKHOUSE_CLIENT --query="DROP TABLE test_constraints;"

# Test two constraints on one table
$CLICKHOUSE_CLIENT --query="CREATE TABLE test_constraints
(
        a       UInt32,
        b       UInt32,
        CONSTRAINT a_constraint CHECK a < 10,
        CONSTRAINT b_constraint CHECK b > 10
)
ENGINE = MergeTree ORDER BY (a);"

# This one must throw an exception
EXCEPTION_TEXT="Violated constraint b_constraint in table test_constraints at indices {0}"
$CLICKHOUSE_CLIENT --query="INSERT INTO test_constraints VALUES (1, 2);" 2>&1 \
    | grep -q "$EXCEPTION_TEXT" && echo "$EXCEPTION_SUCCESS_TEXT" || echo "Did not thrown an exception"
$CLICKHOUSE_CLIENT --query="SELECT * FROM test_constraints;"

# This one  must throw an exception
EXCEPTION_TEXT="Violated constraint a_constraint in table test_constraints at indices {1}"
$CLICKHOUSE_CLIENT --query="INSERT INTO test_constraints VALUES (5, 16), (10, 11), (9, 11), (8, 12);" 2>&1 \
    | grep -q "$EXCEPTION_TEXT" && echo "$EXCEPTION_SUCCESS_TEXT" || echo "Did not thrown an exception"
$CLICKHOUSE_CLIENT --query="SELECT * FROM test_constraints;"

# This one must succeed
$CLICKHOUSE_CLIENT --query="INSERT INTO test_constraints VALUES (7, 18), (0, 11);"
$CLICKHOUSE_CLIENT --query="SELECT * FROM test_constraints;"

$CLICKHOUSE_CLIENT --query="DROP TABLE test_constraints;"