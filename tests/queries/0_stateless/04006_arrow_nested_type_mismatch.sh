#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/78538
# Reading Arrow file with Array(Int) into a table with Nested(c1 Int) column
# should not cause a logical error (bad cast from DataTypeNullable to DataTypeTuple).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE_PREFIX="${CLICKHOUSE_USER_FILES_UNIQUE:?}_04006"
FILE_ARROW="${FILE_PREFIX}.arrow"

function cleanup()
{
    rm -f "${FILE_ARROW}"
    ${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS t0_04006"
    ${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS t1_04006"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --query="CREATE TABLE t0_04006 (c0 Nested(c1 Int), c2 Tuple(c3 Int)) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE t1_04006 (c0 Array(Int)) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query="INSERT INTO TABLE t1_04006 (c0) VALUES ([])"

${CLICKHOUSE_CLIENT} --query="INSERT INTO TABLE FUNCTION file('${FILE_ARROW}', 'Arrow', 'c0 Array(Int)') SELECT c0 FROM t1_04006"

# This should not throw a logical error about bad cast from DataTypeNullable to DataTypeTuple.
${CLICKHOUSE_CLIENT} --query="INSERT INTO TABLE t0_04006 (c0.c1, c2) FROM INFILE '${FILE_ARROW}' FORMAT Arrow" 2>&1 | grep -c 'LOGICAL_ERROR' || true

echo 'OK'
