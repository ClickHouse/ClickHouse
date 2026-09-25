#!/usr/bin/env bash
# A stored table whose column type names AggregateFunction(sequenceNextNode, ...) could be created
# but not opened again: outside a query context the factory passes no settings, and the gate refused
# instead of reading the server's configured value.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

data_path="$CLICKHOUSE_TMP/05261_aggregate_state_attach"
rm -rf "${data_path:?}"

$CLICKHOUSE_LOCAL --path "$data_path" --query "
    SET enable_funnel_functions = 1;
    CREATE TABLE t (c AggregateFunction(sequenceNextNode('forward', 'head'), DateTime, Nullable(String), UInt8)) ENGINE = Memory"

# Read the table rather than a constant, so that opening it is what the output depends on.
$CLICKHOUSE_LOCAL --path "$data_path" --enable_funnel_functions=1 --query "SELECT count() FROM t"

# With the setting off the gate still refuses: it is evaluated, not skipped.
$CLICKHOUSE_LOCAL --path "$data_path" --query "SELECT count() FROM t" 2>&1 \
    | grep -om1 'Set .enable_funnel_functions. setting to enable it'

rm -rf "${data_path:?}"
