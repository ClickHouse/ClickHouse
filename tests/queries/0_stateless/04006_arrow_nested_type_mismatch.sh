#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/78538
# Reading an Arrow file with Array(Int) into a table with Nested column
# should not cause a logical error (bad cast from DataTypeNullable to DataTypeTuple).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA_FILE=${CLICKHOUSE_TEST_UNIQUE_NAME}.arrow

$CLICKHOUSE_LOCAL -q "SELECT [] :: Array(Int32) AS c0 FORMAT Arrow" > "$DATA_FILE"
$CLICKHOUSE_LOCAL -q "
    CREATE TABLE t0 (c0 Nested(c1 Int), c2 Tuple(c3 Int)) ENGINE = Memory;
    INSERT INTO t0 (c0.c1, c2) FROM INFILE '$DATA_FILE' FORMAT Arrow;
    SELECT * FROM t0;
" 2>&1 | grep -c "BAD_CAST\|LOGICAL_ERROR\|Bad cast"

rm -f "$DATA_FILE"
