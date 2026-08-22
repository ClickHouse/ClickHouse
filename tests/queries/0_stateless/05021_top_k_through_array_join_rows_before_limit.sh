#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `rows_before_limit_at_least` remains the number of expanded rows actually seen by the outer
# query limit. With the optimization enabled only the three selected input rows are expanded, so
# this lower bound decreases from all 5000 expanded rows to 15. `exact_rows_before_limit` refuses
# the rewrite and is covered by the EXPLAIN test.

${CLICKHOUSE_CLIENT} --query "
DROP TABLE IF EXISTS t_aj_rbl;
CREATE TABLE t_aj_rbl (x UInt64, arr Array(UInt32)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_aj_rbl SELECT number, range(1, 6) FROM numbers(1000);
"

for setting in 0 1; do
    for kind in "ARRAY JOIN" "LEFT ARRAY JOIN"; do
        echo -n "${kind}, query_plan_top_k_through_array_join = ${setting}: "
        ${CLICKHOUSE_CLIENT} --query "
            SELECT x FROM t_aj_rbl ${kind} arr ORDER BY x LIMIT 3 FORMAT JSONCompact
            SETTINGS query_plan_top_k_through_array_join = ${setting},
                     query_plan_push_down_limit_through_array_join = 0,
                     query_plan_max_limit_for_top_k_optimization = 0,
                     max_block_size = 100
        " | grep -o '"rows_before_limit_at_least": [0-9]*'
    done
done

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_aj_rbl"
