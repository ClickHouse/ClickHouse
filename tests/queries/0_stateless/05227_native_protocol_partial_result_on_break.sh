#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Over the native protocol the result is pulled from a `LazyOutputFormat` by the connection thread, chunk by chunk.
# When the time limit was exceeded before the next pull, the rows that the pipeline had already put into the
# format were dropped, and the end of the data was reported without them: with `timeout_overflow_mode = 'break'`
# the client got a truncated partial result, and with 'throw' it got a successful, empty result instead of
# `TIMEOUT_EXCEEDED`.
#
# The failpoint delays the first pull of a query whose id starts with the prefix by two seconds, which is longer
# than `max_execution_time`; the query itself finishes at once, so the whole result is sitting in the format when
# the connection thread finally pulls it.
#
# The failpoint is deliberately left enabled at the end: it is global to the server, so disabling it would remove
# the delay from a concurrently running copy of this test (the flaky check runs it in parallel with itself), which
# then gets its result before the time limit. It affects only the queries whose id starts with the prefix.

FP=pulling_async_pipeline_executor_delay_first_pull
QID_PREFIX=pulling_async_pipeline_executor_delay_first_pull_

${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT $FP"

echo "-- break: the rows that were already produced are returned"
${CLICKHOUSE_CLIENT} --query_id "${QID_PREFIX}${CLICKHOUSE_TEST_UNIQUE_NAME}_break" \
    --max_execution_time 1 --timeout_overflow_mode break --max_block_size 2 \
    --query "SELECT number FROM numbers(5)"

echo "-- throw: the time limit is reported"
${CLICKHOUSE_CLIENT} --query_id "${QID_PREFIX}${CLICKHOUSE_TEST_UNIQUE_NAME}_throw" \
    --max_execution_time 1 --timeout_overflow_mode throw --max_block_size 2 \
    --query "SELECT number FROM numbers(5)" 2>&1 | grep -o -m1 "TIMEOUT_EXCEEDED"
