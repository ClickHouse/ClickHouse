#!/usr/bin/env bash
# A table with a column of type AggregateFunction(sequenceNextNode, ...) could be created
# but not opened again.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

data_path="$CLICKHOUSE_TMP/05252_aggregate_state_attach"
rm -rf "${data_path:?}"

$CLICKHOUSE_LOCAL --path "$data_path" --query "
    SET enable_funnel_functions = 1;
    CREATE TABLE t_sequence_next_node (c AggregateFunction(sequenceNextNode('forward', 'head'), DateTime, Nullable(String), UInt8)) ENGINE = Memory"

# Read the table rather than a constant, so that opening it is what the output depends on.
$CLICKHOUSE_LOCAL --path "$data_path" --query "
    SELECT count() FROM t_sequence_next_node"

rm -rf "${data_path:?}"
