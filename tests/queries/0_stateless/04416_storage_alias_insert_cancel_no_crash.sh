#!/usr/bin/env bash
# Tags: no-fasttest
# Inserting through an Alias table runs a nested INSERT pipeline inside AliasSink.
# When the outer query is cancelled without an exception (timeout_overflow_mode='break'),
# AliasSink must finalize that nested executor; otherwise ~PushingPipelineExecutor aborts
# with the assertion 'finished || std::uncaught_exceptions() || std::current_exception()'.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE alias_cancel_target (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE alias_cancel ENGINE = Alias('alias_cancel_target');
"

# The SELECT is throttled with sleepEachRow so the 0.3s execution timeout reliably fires
# while the nested AliasSink insert pipeline is still running. timeout_overflow_mode='break'
# cancels the pipeline without raising an exception. Before the fix this crashed the server.
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO alias_cancel
        SELECT number FROM numbers(100) WHERE sleepEachRow(0.05) = 0
        SETTINGS max_execution_time = 0.3, timeout_overflow_mode = 'break', max_block_size = 1;
" 2>&1 | grep -vF 'survived' ||:

# The server is still alive: a follow-up query succeeds.
${CLICKHOUSE_CLIENT} --query "SELECT 'survived'"

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE alias_cancel;
    DROP TABLE alias_cancel_target;
"
