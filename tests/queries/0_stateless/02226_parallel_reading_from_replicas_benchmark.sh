#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -m -q "
drop table if exists data_02226;
create table data_02226 (key Int) engine=MergeTree() order by key
as select * from numbers(1);
"

# Regression for:
#
#   Logical error: 'Coordinator for parallel reading from replicas is not initialized'.
opts=(
    --enable_parallel_replicas 1
    --parallel_replicas_for_non_replicated_merge_tree 1
    --max_parallel_replicas 3
    --cluster_for_parallel_replicas parallel_replicas

    --iterations 1
)
$CLICKHOUSE_BENCHMARK --query "select * from remote('127.1', $CLICKHOUSE_DATABASE, data_02226)" "${opts[@]}" >& /dev/null
ret=$?

$CLICKHOUSE_CLIENT -m -q "
drop table data_02226;
"

exit $ret
