#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `rows_before_limit_at_least` counts rows as seen by the sort that feeds the LIMIT. The rewrite
# keeps the outer `Sorting` in place precisely so that this counter still observes the expanded
# rows; it must report the same value with the optimization on and off.

${CLICKHOUSE_CLIENT} --query "
DROP TABLE IF EXISTS t_aj_rbl;
CREATE TABLE t_aj_rbl (x UInt64, arr Array(UInt32)) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_aj_rbl SELECT number, range(number % 5) FROM numbers(1000);
"

for setting in 0 1; do
    for kind in "ARRAY JOIN" "LEFT ARRAY JOIN"; do
        echo -n "${kind}, query_plan_top_k_through_array_join = ${setting}: "
        ${CLICKHOUSE_CLIENT} --query "
            SELECT x FROM t_aj_rbl ${kind} arr ORDER BY x LIMIT 3 FORMAT JSONCompact
            SETTINGS query_plan_top_k_through_array_join = ${setting}, max_block_size = 100
        " | grep -o '"rows_before_limit_at_least": [0-9]*'
    done
done

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_aj_rbl"
