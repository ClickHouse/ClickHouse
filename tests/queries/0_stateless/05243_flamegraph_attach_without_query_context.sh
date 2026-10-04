#!/usr/bin/env bash
# A stored table whose column type names AggregateFunction(flameGraph, ...) used to crash the
# process when a second run opened it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

cd "${CLICKHOUSE_TMP}" || exit
rm -rf "05243_flamegraph_attach"

$CLICKHOUSE_LOCAL --path "05243_flamegraph_attach" --query "SET allow_introspection_functions = 1; CREATE TABLE t (c AggregateFunction(flameGraph, Array(UInt64))) ENGINE = Memory"

# Read the table rather than a constant, so that opening it is what the output depends on.
$CLICKHOUSE_LOCAL --path "05243_flamegraph_attach" --query "SELECT count() FROM t"

rm -rf "05243_flamegraph_attach"
