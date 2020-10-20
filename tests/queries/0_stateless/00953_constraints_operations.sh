#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

EXCEPTION_TEXT=violated
EXCEPTION_SUCCESS_TEXT=ok

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

$CLICKHOUSE_CLIENT --query="INSERT INTO test_constraints VALUES (1, 0);" 2>&1 \
    | grep -q "$EXCEPTION_TEXT" && echo "$EXCEPTION_SUCCESS_TEXT" || echo "Did not thrown an exception"
$CLICKHOUSE_CLIENT --query="SELECT * FROM test_constraints;"

$CLICKHOUSE_CLIENT --query="ALTER TABLE test_constraints DROP CONSTRAINT b_constraint;"

# This one must suceed now
$CLICKHOUSE_CLIENT --query="INSERT INTO test_constraints VALUES (1, 0);"

$CLICKHOUSE_CLIENT --query="ALTER TABLE test_constraints ADD CONSTRAINT b_constraint CHECK b > 10;"

$CLICKHOUSE_CLIENT --query="INSERT INTO test_constraints VALUES (1, 10);" 2>&1 \
    | grep -q "$EXCEPTION_TEXT" && echo "$EXCEPTION_SUCCESS_TEXT" || echo "Did not thrown an exception"

$CLICKHOUSE_CLIENT --query="INSERT INTO test_constraints VALUES (1, 11);"

$CLICKHOUSE_CLIENT --query="DROP TABLE test_constraints;"