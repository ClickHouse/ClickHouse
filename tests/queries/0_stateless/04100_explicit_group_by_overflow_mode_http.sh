#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# An explicit `group_by_overflow_mode = 'throw'` passed as a URL parameter must not be capped by `optimize_trivial_group_by_limit_query`.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&max_rows_to_group_by=50&group_by_overflow_mode=throw&max_threads=1&max_block_size=10" \
    --data-binary 'SELECT number FROM numbers(1000) GROUP BY number LIMIT 5' 2>&1 | grep -o 'TOO_MANY_ROWS' | head -1
