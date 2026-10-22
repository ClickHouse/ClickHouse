#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./05293_window_functions_block_boundaries.lib
. "$CUR_DIR"/05293_window_functions_block_boundaries.lib

create_table

{
check 'no partition by' "SELECT p, id, sum(v) OVER (ORDER BY k RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS r FROM t"
check 'no order by' "SELECT p, id, sum(v) OVER (PARTITION BY p) AS r FROM t"
check 'single row partitions' "SELECT p, id, sum(v) OVER (PARTITION BY id ORDER BY k ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS r FROM t"
} | $CLICKHOUSE_CLIENT
