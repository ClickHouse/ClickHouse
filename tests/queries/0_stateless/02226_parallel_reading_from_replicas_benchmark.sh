#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -nm -q "
drop table if exists data_02226;
create table data_02226 (key Int) engine=MergeTree() order by key
as select * from numbers(1);
"

# Regression for:
#
#   Logical error: 'Coordinator for parallel reading from replicas is not initialized'.
opts=(
    --allow_experimental_parallel_reading_from_replicas 1
    --max_parallel_replicas 3

    --iterations 1
)
$CLICKHOUSE_BENCHMARK --query "select * from remote('127.1', $CLICKHOUSE_DATABASE, data_02226)" "${opts[@]}" >& /dev/null
ret=$?

$CLICKHOUSE_CLIENT -nm -q "
drop table data_02226;
"

exit $ret
