#!/usr/bin/env bash

# Test that ORDER BY ... WITH FILL respects max_execution_time and stops promptly.
# A single WITH FILL range can expand into billions of rows inside one transform() call, and the
# pipeline executor only enforces max_execution_time between work() calls, so without checking the
# time limit inside the generation loops the query runs unbounded and ignores the limit.
# Ref: https://github.com/ClickHouse/ClickHouse/issues/61713

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `timeout_overflow_mode = 'throw'`: the query must stop with a timeout after ~1 second instead of
# filling ~3 billion rows (year 2000 to 2100, step 1 second) and running for minutes.
# The query watchdog may cancel the query with `QUERY_WAS_CANCELLED` before the pipeline's own
# time-limit check throws `TIMEOUT_EXCEEDED`; both mean the limit was enforced, so normalize them.
${CLICKHOUSE_CLIENT} --query "
    SELECT ts
    FROM (SELECT toDateTime('2050-06-15 12:00:00') AS ts)
    ORDER BY ts WITH FILL FROM toDateTime('2000-01-01 00:00:00') TO toDateTime('2100-01-01 00:00:00') STEP 1
    SETTINGS max_execution_time = 1, timeout_overflow_mode = 'throw'
    FORMAT Null" 2>&1 | grep -ow -m1 -E 'TIMEOUT_EXCEEDED|QUERY_WAS_CANCELLED' | sed 's/QUERY_WAS_CANCELLED/TIMEOUT_EXCEEDED/'

# `timeout_overflow_mode = 'break'`: the soft timeout does not throw, and during a single long
# transform() call nothing enforces it inside the transform: the executor and the client polling loop
# observe the limit only between work() calls / result chunks, and the cancellation checker leaves
# break-mode queries running. The client polling loop does cancel the pipeline soon after the deadline
# (setting `isCancelled`), but breaking on `isCancelled` in the inner generation loops only ends the
# current gap; the outer loops over ranges and over input rows keep refilling every remaining
# range/row (each up to DEFAULT_BLOCK_SIZE rows before the next `isCancelled` check fires). This slow
# drain produces no error and stays under the memory limit, but takes tens of seconds (minutes under
# sanitizers) instead of ~1 second, so the bug is only observable through the query duration.
#
# Without any cancellation checks in the generation loops at all, the fill would run truly unbounded;
# `max_memory_usage` bounds that worst case so a full regression fails fast instead of hanging.
#
# The fixed version checks the time limit inside the generation loops and stops in ~1 second, so the
# 15-second duration threshold separates it reliably from the buggy behavior on any build type.

# Many sorting-prefix groups, each expanding to a huge suffix: exercises the outer loop over ranges in
# transform() (it must stop instead of refilling the remaining groups).
${CLICKHOUSE_CLIENT} --query "
    SELECT g, x
    FROM (SELECT number AS g, 0::UInt64 AS x FROM numbers(100000))
    ORDER BY g, x WITH FILL FROM 0 TO 1000000000000 STEP 1
    SETTINGS max_execution_time = 1, timeout_overflow_mode = 'break', use_with_fill_by_sorting_prefix = 1,
        max_memory_usage = 4000000000, log_comment = '04538_fill_break_ranges'
    FORMAT Null"

# Many input rows in a single range, each separated by a huge gap: exercises the outer loop over input
# rows in transformRange() (it must stop instead of refilling the gaps after the remaining rows).
${CLICKHOUSE_CLIENT} --query "
    SELECT x
    FROM (SELECT (number * 10000000000)::UInt64 AS x FROM numbers(100000))
    ORDER BY x WITH FILL FROM 0 TO 1000000000000000 STEP 1
    SETTINGS max_execution_time = 1, timeout_overflow_mode = 'break',
        max_memory_usage = 4000000000, log_comment = '04538_fill_break_rows'
    FORMAT Null"

# Many sorting-prefix groups whose first fill key is above FROM, so WITH FILL emits an initial fill_from
# row for each group. Once the break-mode timeout fires while generating a group's suffix, transformRange()
# must not start the next group at all - not even its fill_from preamble row - and must not repoint
# last_range_sort_prefix at a group whose original rows were never consumed. Otherwise the partial result
# leaks rows from a group past the deadline. The stopping point is wall-clock dependent, so we assert the
# same prompt-stop duration property here (the guarded preamble path must not slow the query down).
${CLICKHOUSE_CLIENT} --query "
    SELECT g, x
    FROM (SELECT number AS g, 1000000::UInt64 AS x FROM numbers(100000))
    ORDER BY g, x WITH FILL FROM 0 TO 1000000000000 STEP 1
    SETTINGS max_execution_time = 1, timeout_overflow_mode = 'break', use_with_fill_by_sorting_prefix = 1,
        max_memory_usage = 4000000000, log_comment = '04538_fill_break_prefix_above_from'
    FORMAT Null"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

${CLICKHOUSE_CLIENT} --query "
    SELECT log_comment, if(query_duration_ms < 15000, 'OK', 'SLOW: ' || toString(query_duration_ms) || ' ms')
    FROM system.query_log
    WHERE current_database = currentDatabase()
        AND event_date >= yesterday()
        AND type = 'QueryFinish'
        AND log_comment IN ('04538_fill_break_ranges', '04538_fill_break_rows', '04538_fill_break_prefix_above_from')
    ORDER BY log_comment"
