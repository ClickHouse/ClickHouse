#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

err="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.err"

$CLICKHOUSE_LOCAL -q "create table t (v UInt32) engine = Null; insert into t values (1); select 'after insert'; aaa ... this is ignored" 2>"$err"
echo "exit code: $?"
grep -oF -m1 SYNTAX_ERROR "$err"
rm -f "$err"
