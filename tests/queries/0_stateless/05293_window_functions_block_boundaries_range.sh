#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./05293_window_functions_block_boundaries.lib
. "$CUR_DIR"/05293_window_functions_block_boundaries.lib

create_table

{
check 'range default frame' "SELECT p, id, sum(v) OVER (PARTITION BY p ORDER BY k) AS r FROM t"
check 'range 2 preceding 2 following' "SELECT p, id, sum(v) OVER (PARTITION BY p ORDER BY k RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS r FROM t"
check 'range 1 following 3 following' "SELECT p, id, sum(v) OVER (PARTITION BY p ORDER BY k RANGE BETWEEN 1 FOLLOWING AND 3 FOLLOWING) AS r FROM t"
check 'range current row unbounded following' "SELECT p, id, sum(v) OVER (PARTITION BY p ORDER BY k RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS r FROM t"
check 'range desc 2 preceding 1 following' "SELECT p, id, sum(v) OVER (PARTITION BY p ORDER BY k DESC RANGE BETWEEN 2 PRECEDING AND 1 FOLLOWING) AS r FROM t"
check 'range nulls last 1 preceding 1 following' "SELECT p, id, sum(v) OVER (PARTITION BY p ORDER BY k ASC NULLS LAST RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS r FROM t"
} | $CLICKHOUSE_CLIENT
