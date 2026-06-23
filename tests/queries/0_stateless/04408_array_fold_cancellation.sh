#!/usr/bin/env bash
# Tags: long, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# arrayFold folds an entire array inside a single executeImpl() call, so the pipeline-level time check
# (which only runs between blocks) cannot interrupt a fold over one very long array. Without the in-function
# check this PR adds, such a fold ignores max_execution_time and runs to completion regardless of the limit.
# The lambda grows the accumulator (arrayPushFront), so a 1000000-element fold is O(N^2) and takes minutes
# uninterrupted -- the original report saw it run for 2709s after cancellation.
#
# Both queries set max_execution_time=1 and are wrapped in `timeout 60`. With the fix each one stops in a
# couple of seconds; without it they run for minutes, `timeout` kills the client, and the output differs.
ARR="range(materialize(toUInt64(1000000)))"
FOLD="arrayFold((acc, x) -> arrayPushFront(acc, x), ${ARR}, emptyArrayUInt64())"

# throw mode: the in-function check throws TIMEOUT_EXCEEDED promptly.
timeout 60 ${CLICKHOUSE_CLIENT} --max_execution_time 1 --timeout_overflow_mode throw \
    --query "SELECT ignore(${FOLD})" 2>&1 | grep -o -m1 "TIMEOUT_EXCEEDED" || echo "throw: no timeout"

# break mode: checkTimeLimit() returns false instead of throwing. A fold has no meaningful partial result, so
# the in-function check stops it; the pipeline absorbs the stop and the query ends without an error. This is
# the path the code ignored before this PR (it discarded the false return and kept folding to completion).
timeout 60 ${CLICKHOUSE_CLIENT} --max_execution_time 1 --timeout_overflow_mode break \
    --query "SELECT ignore(${FOLD})" > /dev/null 2>&1 && echo "break: stopped without error" || echo "break: unexpected failure"
