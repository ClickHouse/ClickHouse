#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./05293_window_functions_block_boundaries.lib
. "$CUR_DIR"/05293_window_functions_block_boundaries.lib

create_table

{
check 'rows 3 preceding 2 following' "SELECT p, id, sum(v) OVER (PARTITION BY p ORDER BY k, id ROWS BETWEEN 3 PRECEDING AND 2 FOLLOWING) AS r FROM t"
check 'rows 2 following 5 following' "SELECT p, id, sum(v) OVER (PARTITION BY p ORDER BY k, id ROWS BETWEEN 2 FOLLOWING AND 5 FOLLOWING) AS r FROM t"
check 'rows 5 preceding 2 preceding' "SELECT p, id, sum(v) OVER (PARTITION BY p ORDER BY k, id ROWS BETWEEN 5 PRECEDING AND 2 PRECEDING) AS r FROM t"
check 'rows unbounded preceding current row' "SELECT p, id, sum(v) OVER (PARTITION BY p ORDER BY k, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS r FROM t"
check 'rows current row unbounded following' "SELECT p, id, sum(v) OVER (PARTITION BY p ORDER BY k, id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS r FROM t"
check 'rows 100 preceding 100 following' "SELECT p, id, sum(v) OVER (PARTITION BY p ORDER BY k, id ROWS BETWEEN 100 PRECEDING AND 100 FOLLOWING) AS r FROM t"
} | $CLICKHOUSE_CLIENT
