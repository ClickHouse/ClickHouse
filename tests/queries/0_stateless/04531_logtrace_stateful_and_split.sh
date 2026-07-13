#!/usr/bin/env bash
# Tags: race

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=trace

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `logTrace` must run for every block even inside a `WHERE ... AND ...` chain. With `query_plan_merge_filters`
# enabled (the default), `FilterStep` splits the `AND` into separate filter transforms and evaluates the leading
# `number % 2 = 0` condition first, so without marking `logTrace` stateful it would only see the blocks that
# survive that condition and log fewer times than there are input blocks (two instead of four here). Marking
# `logTrace` stateful disables the split. With one row per block and four blocks, all four must produce a trace
# message, regardless of the leading condition that keeps only two of the rows.
${CLICKHOUSE_CLIENT} --query="
    SELECT count() FROM numbers(4) WHERE (number % 2 = 0) AND (logTrace('test_04531') = 0)
    SETTINGS max_block_size = 1, max_threads = 1, query_plan_merge_filters = 1 FORMAT Null
" 2>&1 | grep -c "FunctionLogTrace: test_04531"
