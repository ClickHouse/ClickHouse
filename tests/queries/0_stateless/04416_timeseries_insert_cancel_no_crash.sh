#!/usr/bin/env bash
# Tags: no-fasttest
# Inserting into a TimeSeries table runs nested INSERT pipelines inside `TimeSeriesSink`.
# When the outer query is cancelled without an exception (`timeout_overflow_mode='break'`),
# `TimeSeriesSink` must finalize those nested executors; otherwise `~PushingPipelineExecutor`
# aborts with `finished || std::uncaught_exceptions() || std::current_exception()`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_time_series_table = 1;
    CREATE TABLE ts_cancel ENGINE = TimeSeries;
"

# The SELECT is throttled with `sleepEachRow` so the 0.3s execution timeout reliably fires
# while the nested `TimeSeriesSink` insert pipelines are still running. `timeout_overflow_mode='break'`
# cancels the pipeline without raising an exception. Before the fix this aborted the server.
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO ts_cancel (metric_name, tags, time_series)
        SELECT
            'cpu' || toString(number) AS metric_name,
            map('n', toString(number)) AS tags,
            [(toDateTime64(number, 3), toFloat64(number))] AS time_series
        FROM numbers(100) WHERE sleepEachRow(0.05) = 0
        SETTINGS max_execution_time = 0.3, timeout_overflow_mode = 'break', max_block_size = 1;
" 2>&1 | grep -vF 'survived' ||:

# The server is still alive: a follow-up query succeeds.
${CLICKHOUSE_CLIENT} --query "SELECT 'survived'"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ts_cancel;"
