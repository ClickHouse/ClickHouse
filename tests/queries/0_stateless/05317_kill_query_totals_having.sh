#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: the test needs a build with failpoints.
# Tag no-parallel: `TotalsHavingTransform` parks the query inside a server-global PAUSEABLE
# failpoint, so a concurrent test instance pausing or resuming the same channel would break the
# synchronisation.

# Test that `KILL QUERY` stops a `GROUP BY ... WITH TOTALS` query while the aggregate states of a
# chunk are being accumulated into the totals row.
#
# Regression test: `TotalsHavingTransform::addToTotals` merged every row of every aggregate column
# unconditionally, and the transform also merged a chunk that had arrived after the query was
# already cancelled and still prepared the totals afterwards, so a `KILL QUERY` landing in the
# middle of the accumulation only took effect once the whole chunk - and the totals - were done.
#
# The transform now polls the cancellation every 4096 rows of the loop, and drops the chunk and
# the totals preparation as soon as the query is cancelled. Both loops of `addToTotals` are
# covered: the plain query accumulates without a filter (`BEFORE_HAVING`) and the `HAVING` query
# accumulates with the filter of the `HAVING` expression (`AFTER_HAVING_EXCLUSIVE`, the default
# `totals_mode`).
#
# The drop after the poll is asserted positively: the release below resumes the loop while the
# query is already cancelled, and the drop path of `transform` parks at a second failpoint. The
# wait for that pause must succeed, so a build which still filters and emits the cancelled chunk
# after the loop returned fails instead of passing on the old evidence alone.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

FP=totals_having_transform_pause
DROP_FP=totals_having_transform_drop_cancelled_chunk
TOTALS_QID="totals_kill_${CLICKHOUSE_DATABASE}_$$"
HAVING_QID="totals_having_kill_${CLICKHOUSE_DATABASE}_$$"

# Two-level aggregation emits the accumulated aggregate states as one chunk per bucket of 256, so
# a small group count would never reach the 4096-row boundary where the failpoint sits. Force a
# single-level aggregation instead: it converts the whole aggregation result into one chunk, which
# keeps the group count (and with it the memory and the runtime of this test) small.
# Both thresholds of `worthConvertToTwoLevel()` have to be disabled: it switches to two levels when
# `group_by_two_level_threshold` OR `group_by_two_level_threshold_bytes` is exceeded, and the
# stateless harness randomizes the latter, so zeroing only the former makes this test pass locally
# and time out on CI.
SETTINGS_SUFFIX="group_by_two_level_threshold=0, group_by_two_level_threshold_bytes=0, enable_adaptive_aggregator=0, max_threads=1, max_rows_to_read=0"

TOTALS_QUERY="SELECT intDiv(number, 10) AS k, count() AS cnt, sum(number) AS total
FROM numbers(1000000)
GROUP BY k WITH TOTALS
FORMAT Null
SETTINGS ${SETTINGS_SUFFIX}"

TOTALS_HAVING_QUERY="SELECT intDiv(number, 10) AS k, count() AS cnt, sum(number) AS total
FROM numbers(1000000)
GROUP BY k WITH TOTALS
HAVING k % 2 = 0
FORMAT Null
SETTINGS ${SETTINGS_SUFFIX}"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT ${FP}" 2>/dev/null ||:
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT ${DROP_FP}" 2>/dev/null ||:
    $CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id IN ('${TOTALS_QID}', '${HAVING_QID}') FORMAT Null" 2>/dev/null ||:
    wait 2>/dev/null ||:
}
trap cleanup EXIT

## Kill the query while it accumulates the aggregate states of a chunk into the totals row.
## The transform pauses at the first 4096-row boundary inside the accumulation loop (the
## `PAUSEABLE_ONCE` failpoint auto-disables after that one pause). The query is killed while it
## sits there and then released again. The cancellation is polled right after the pause, so the
## loop returns at that row and never reaches the next 4096-row boundary, which proves the loop
## polls the cancellation instead of merging the rest of the chunk. Without the polling the loop
## would hit the re-armed failpoint at the next boundary and the second wait below would return.
function kill_during_totals_accumulation()
{
    local label="$1"
    local query_id="$2"
    local query="$3"

    $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT ${FP}"

    $CLICKHOUSE_CLIENT --query_id "${query_id}" --query "${query}" > /dev/null 2>&1 &
    local query_pid=$!

    ## Bounded, because a build which does not reach the loop - or a chunk with too few aggregate
    ## states for the boundary - never pauses at all, and an unbounded wait would spend the whole
    ## test timeout discovering that instead of failing with a diagnostic.
    if ! timeout 30 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT ${FP} PAUSE" > /dev/null 2>&1; then
        echo "FAIL: ${label} query never paused in the totals accumulation loop"
        exit 1
    fi

    ## Assert the precondition instead of inferring it: the query must be parked at the failpoint,
    ## so it is still in system.processes before the kill can mean anything.
    local parked
    parked=$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.processes WHERE query_id = '${query_id}'")
    if [ "${parked}" != "1" ]; then
        echo "FAIL: the paused totals accumulation does not belong to the ${label} query"
        exit 1
    fi

    ## ASYNC sets the cancellation flag before this statement returns, so the loop observes it as
    ## soon as it is released. A SYNC kill would instead wait for the query to terminate, which
    ## cannot happen before the release below.
    $CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '${query_id}' ASYNC FORMAT Null" > /dev/null

    ## Re-arm the one-shot failpoint and only then resume the loop, so that the loop pausing again
    ## at the next boundary is observable by the second wait below. Arm the drop failpoint as well:
    ## the release resumes a loop which is cancelled by now, so its next cancellation check in
    ## `transform` drops the chunk and parks there, which is asserted below.
    $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT ${FP}"
    $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT ${DROP_FP}"
    $CLICKHOUSE_CLIENT --query "SYSTEM NOTIFY FAILPOINT ${FP}"

    local pause_rc=0
    timeout 10 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT ${FP} PAUSE" > /dev/null 2>&1 || pause_rc=$?
    if [ "${pause_rc}" -eq 0 ]; then
        echo "FAIL: ${label} query paused again after the kill, so the accumulation loop does not poll the cancellation"
        exit 1
    elif [ "${pause_rc}" -ne 124 ]; then
        echo "FAIL: the second wait failed with status ${pause_rc}"
        exit 1
    fi
    echo "${label}: the accumulation loop stopped at the cancelled row"

    ## The loop stopped at the cancelled row - now prove that the resumed transform does not keep
    ## the chunk: the drop path of `transform` parks at this failpoint before clearing the chunk
    ## and stopping the reading, and the wait for that pause has to succeed. The query is cancelled
    ## by now, so a build which filters and emits the chunk instead never reaches it. The wait is
    ## already satisfied here, because the drop happened while the assertion above waited out its
    ## timeout, so the fixed case costs no extra time.
    if ! timeout 10 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT ${DROP_FP} PAUSE" > /dev/null 2>&1; then
        echo "FAIL: ${label} query did not drop the cancelled chunk after the accumulation loop stopped"
        exit 1
    fi
    $CLICKHOUSE_CLIENT --query "SYSTEM NOTIFY FAILPOINT ${DROP_FP}"

    ## DISABLE releases the query if it is still parked and disarms the failpoint for the tests
    ## which run after this one.
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT ${FP}"
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT ${DROP_FP}"

    timeout 60 tail --pid="${query_pid}" -f /dev/null
    wait "${query_pid}" 2>/dev/null ||:

    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.query_log
        WHERE query_id = '${query_id}' AND current_database = currentDatabase() AND exception_code = 394
    "
}

kill_during_totals_accumulation "totals" "${TOTALS_QID}" "${TOTALS_QUERY}"
kill_during_totals_accumulation "totals_having" "${HAVING_QID}" "${TOTALS_HAVING_QUERY}"
