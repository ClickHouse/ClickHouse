#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

NATIVE_FILE=$(mktemp "$CLICKHOUSE_TMP/native_XXXXXX.native")
NATIVE_FILE2=$(mktemp "$CLICKHOUSE_TMP/native2_XXXXXX.native")
trap "rm -f $NATIVE_FILE $NATIVE_FILE2" EXIT

$CLICKHOUSE_LOCAL -q "SELECT number AS a, toString(number) AS b FROM numbers(5) FORMAT Native" > "$NATIVE_FILE"

cat "$NATIVE_FILE" | $CLICKHOUSE_LOCAL -q "
    CREATE TABLE t ENGINE=Memory AS SELECT * FROM input('auto', 'Native');
    SELECT * FROM t ORDER BY a;
"

cat "$NATIVE_FILE" | $CLICKHOUSE_LOCAL -q "
    CREATE TABLE t (a UInt64, b String) ENGINE=Memory
        AS SELECT * FROM input('a UInt64, b String', 'Native');
    SELECT * FROM t ORDER BY a;
"

cat "$NATIVE_FILE" | $CLICKHOUSE_LOCAL \
    --query="CREATE TABLE t (a UInt64, b String) ENGINE=Memory" \
    --query="INSERT INTO t SELECT * FROM input() FORMAT Native" \
    --query="SELECT * FROM t ORDER BY a"

$CLICKHOUSE_LOCAL -q "SELECT toFloat64(number) AS x, toDate('2020-01-01') + number AS d FROM numbers(3) FORMAT Native" > "$NATIVE_FILE2"
cat "$NATIVE_FILE2" | $CLICKHOUSE_LOCAL -q "
    CREATE TABLE t ENGINE=Memory AS SELECT * FROM input('auto', 'Native');
    SELECT * FROM t ORDER BY x;
"

cat "$NATIVE_FILE" | $CLICKHOUSE_LOCAL -q "SELECT * FROM input('auto', 'Native') ORDER BY a"

cat "$NATIVE_FILE" | $CLICKHOUSE_LOCAL -q "
    CREATE TABLE t ENGINE=Memory AS SELECT * FROM input('auto', 'Parquet');
    SELECT * FROM t ORDER BY a;
" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1
