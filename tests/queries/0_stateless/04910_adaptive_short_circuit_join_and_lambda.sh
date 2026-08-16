#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --multiquery <<'EOF'
SET enable_adaptive_short_circuit_lazy_execution = 1;
SET short_circuit_function_evaluation = 'force_enable';
SET max_block_size = 10000;

SELECT count()
FROM numbers(30000) AS left_table
INNER JOIN numbers(30000) AS right_table
    ON left_table.number = right_table.number
    AND and(left_table.number % 2 = 0, left_table.number % 3 = 0);

SELECT arraySum(arrayMap(x -> and(x % 2 = 0, x % 3 = 0), range(30000)));
EOF
