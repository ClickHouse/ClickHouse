#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --multiquery <<'EOF'
SET enable_adaptive_short_circuit_lazy_execution = 1;
SET short_circuit_function_evaluation = 'force_enable';
SET max_block_size = 10000;

WITH intDiv(1, number % 2) AS x
SELECT sum(and(number % 2 != 0, if(1, x, 0)))
FROM numbers(100000);

-- Propagate the profile of an eagerly executed lazy descendant through `x` to its lazy ancestors.
WITH arraySum(arrayMap(x -> x * x, range(number % 100))) AS x
SELECT count()
FROM numbers(50000)
WHERE and(number % 2 != 0, if(number % 3 != 0, x > 0, 0));
EOF
