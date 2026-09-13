#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --multiquery <<'EOF'
SET enable_adaptive_short_circuit_lazy_execution = 1;
SET short_circuit_function_evaluation = 'force_enable';
SET max_block_size = 10000;

-- `arrayJoin` multiplies the number of rows in the middle of the same `ExpressionActions`. The adaptive
-- cost model must be fed the number of rows the actions were executed on, not the post-expansion size,
-- otherwise the eager alternative of the branches under the `if` is costed against an inflated row count.
SELECT count(), sum(x)
FROM
(
    SELECT arrayJoin(if(number % 2 = 0, arrayMap(y -> y * y, range(number % 20)), range(2))) AS x
    FROM numbers(200000)
);

-- The same shape with the expansion below a short-circuit branch: the branch is costed on the expanded
-- rows, and its profile must not be mixed with the pre-expansion row count of the round.
SELECT count()
FROM
(
    SELECT arrayJoin(range(number % 5)) AS x, number
    FROM numbers(200000)
)
WHERE and(number % 2 != 0, arraySum(arrayMap(y -> y * y, range(number % 100))) + x >= 0);

-- The expensive branch throws for the rows the condition filters out, so lazy execution must be kept
-- for correctness no matter how the rows are accounted.
SELECT count()
FROM
(
    SELECT arrayJoin(if(number % 3 != 0, range(intDiv(10, number % 3)), emptyArrayUInt64())) AS x
    FROM numbers(200000)
);
EOF
