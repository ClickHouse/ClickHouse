#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./05293_window_functions_block_boundaries.lib
. "$CUR_DIR"/05293_window_functions_block_boundaries.lib

create_table

{
check 'groups 1 preceding 1 following' "SELECT p, id, sum(v) OVER (PARTITION BY p ORDER BY k GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS r FROM t"
check 'groups 2 following 3 following' "SELECT p, id, sum(v) OVER (PARTITION BY p ORDER BY k GROUPS BETWEEN 2 FOLLOWING AND 3 FOLLOWING) AS r FROM t"
check 'groups 3 preceding 1 preceding' "SELECT p, id, sum(v) OVER (PARTITION BY p ORDER BY k GROUPS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS r FROM t"
check 'groups unbounded preceding current row' "SELECT p, id, sum(v) OVER (PARTITION BY p ORDER BY k GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS r FROM t"
check 'groups current row unbounded following' "SELECT p, id, sum(v) OVER (PARTITION BY p ORDER BY k GROUPS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS r FROM t"
} | $CLICKHOUSE_CLIENT
