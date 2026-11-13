#!/usr/bin/env bash
# Tags: no-tsan
# ^ TSan uses more stack

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Failed readings of a subquery are remembered, so this query fails fast with a syntax error
# instead of exceeding the limit of backtracks (`TOO_SLOW_PARSING`).
$CLICKHOUSE_LOCAL --query "SELECT((((((((((SELECT(((((((((SELECT((((((((((SELECT(((((((((SELECT((((((((((SELECT(((((((((SELECT 1+)))))))))))))))))))))))))))))))))))))))))))))))))))))))))" 2>&1 | grep -o -E 'SYNTAX_ERROR|TOO_SLOW_PARSING'
