#!/usr/bin/env bash
# Tags: race

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=trace

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `logTrace` must not be constant-folded during query analysis: it has to run for every block
# during execution. With one row per block, three blocks must produce three trace messages
# (before the fix it was folded to a constant and logged exactly once during analysis).
${CLICKHOUSE_CLIENT} --query="
    SELECT logTrace('test_04512') FROM numbers(3) SETTINGS max_block_size = 1, max_threads = 1 FORMAT Null
" 2>&1 | grep -c "FunctionLogTrace: test_04512"
