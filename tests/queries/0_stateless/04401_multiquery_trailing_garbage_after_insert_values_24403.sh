#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

out=$($CLICKHOUSE_LOCAL -q "create table t (v UInt32) engine = Null; insert into t values (1); select 'after insert'; aaa ... this is ignored" 2>&1)
rc=$?
echo "$out" | grep -oF -e 'after insert' -e 'SYNTAX_ERROR'
echo "exit code: $rc"
