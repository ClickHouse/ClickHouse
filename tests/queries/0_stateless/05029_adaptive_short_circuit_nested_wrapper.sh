#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --multiquery <<'EOF'
SET enable_adaptive_short_circuit_lazy_execution = 1;
SET short_circuit_function_evaluation = 'force_enable';
SET max_block_size = 10000;

-- An eagerly executed lazy descendant (`heavy`) sits under an `if`, which is not lazily executed itself,
-- and that `if` sits under the lazy `plus`. The cost of `heavy` has to reach `plus` through the wrapper,
-- otherwise `plus` keeps comparing against an underestimated eager cost. Whatever is decided, the results
-- must stay the same.
WITH arraySum(arrayMap(y -> y * y, range(number % 100))) AS heavy
SELECT count()
FROM numbers(200000)
WHERE and(number % 2 != 0, if(number % 3 != 0, heavy, 0) + 1 > 1);

-- The same shape with `multiIf` as the wrapper.
WITH arraySum(arrayMap(y -> y * y, range(number % 100))) AS heavy
SELECT count()
FROM numbers(200000)
WHERE and(number % 2 != 0, multiIf(number % 3 = 0, 0, number % 7 = 0, 0, heavy) + 1 > 1);

-- The descendant throws for the rows which the outer condition filters out, so it must stay lazily
-- executed regardless of how its cost is propagated through the wrapper.
SELECT count()
FROM numbers(200000)
WHERE and(number % 2 != 0, if(number % 3 != 0, intDiv(1, number % 2), 0) + 1 > 1);
EOF
