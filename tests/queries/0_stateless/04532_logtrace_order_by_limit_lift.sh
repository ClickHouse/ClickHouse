#!/usr/bin/env bash
# Tags: race

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=trace

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `logTrace` must run for every input block even when the query has `ORDER BY ... LIMIT`. The
# `tryExecuteFunctionsAfterSorting` optimization lifts projection expressions that are not needed for sorting above
# the `SortingStep` (which carries the pushed-down `LIMIT`); lazy materialization similarly defers them past the
# `LIMIT`. Without marking `logTrace` stateful it would be evaluated only on the single surviving row and log once
# instead of once per input block. Both optimizations now skip stateful functions. With one row per block and four
# blocks, all four must produce a trace message even though the query keeps only one row.
${CLICKHOUSE_CLIENT} --query="
    SELECT logTrace('test_04532'), number FROM numbers(4) ORDER BY number LIMIT 1
    SETTINGS max_block_size = 1, max_threads = 1 FORMAT Null
" 2>&1 | grep -c "FunctionLogTrace: test_04532"
