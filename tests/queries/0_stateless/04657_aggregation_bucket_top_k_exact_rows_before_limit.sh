#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The bucket top-K conversion materializes only each bucket's best `count()` groups, so it must not
# engage when `exact_rows_before_limit` promises an exact pre-limit group count: the
# `rows_before_limit_at_least` counter runs downstream of the conversion and would report the kept
# rows instead of the full number of groups. The query below has 1000 groups, and the counter must
# say so even though the plan shape (final two-level aggregation under `ORDER BY count() DESC
# LIMIT n`) is exactly what the conversion targets.
$CLICKHOUSE_LOCAL --query "
    SELECT toUInt64(number % 1000) AS k, count() AS c FROM numbers(100000)
    GROUP BY k ORDER BY c DESC LIMIT 1
    FORMAT JSON
    SETTINGS exact_rows_before_limit = 1, group_by_two_level_threshold = 1, max_threads = 4, output_format_write_statistics = 0
" | grep -F '"rows_before_limit_at_least"'
