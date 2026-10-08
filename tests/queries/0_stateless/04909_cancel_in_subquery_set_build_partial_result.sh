#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_first_cancel_set_build"

# A partition key makes the minmax_count projection eligible, and `auto_statistics_types = ''` keeps the
# set from being built through the statistics estimation path before the projection gets to it.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_first_cancel_set_build (a Int32, b Int32)
    ENGINE = MergeTree ORDER BY a PARTITION BY a % 3 SETTINGS auto_statistics_types = '';
    INSERT INTO t_first_cancel_set_build SELECT number * 3, number + 1 FROM numbers(15);"

# The minmax_count projection must actually be chosen for the predicate shape under test, otherwise the
# cancellation below never reaches the projection's synchronous filter evaluation and the test would pass
# without exercising anything. All of these are randomized in CI, so they are pinned in the query itself,
# where they override the randomized values, rather than in the probe alone.
PINNED_SETTINGS="optimize_use_projections = 1, optimize_use_implicit_projections = 1,
                 optimize_trivial_count_query = 0, enable_parallel_replicas = 0,
                 use_index_for_in_with_subqueries = 1"

$CLICKHOUSE_CLIENT -q "
    SELECT count() > 0 FROM (
        EXPLAIN indexes = 1
        SELECT count() FROM t_first_cancel_set_build WHERE 1 IN (SELECT number FROM numbers(3))
        SETTINGS $PINNED_SETTINGS
    ) WHERE explain ILIKE '%_minmax_count_projection%'"

# A *constant* left-hand side is essential: it maps to no key column, so primary-key analysis returns
# before building the set, and the projection's filter evaluation is the first materialization attempt.
QUERY_ID="${CLICKHOUSE_DATABASE}_first_cancel_set_build"
CLIENT_ERR="${CLICKHOUSE_TMP}/first_cancel_set_build.err"

$CLICKHOUSE_CLIENT --query_id="$QUERY_ID" -q "
    SELECT count() FROM t_first_cancel_set_build WHERE 1 IN (
        SELECT number FROM numbers(10000000) WHERE sleep(1) = 0
    ) SETTINGS partial_result_on_first_cancel = 1, $PINNED_SETTINGS" > /dev/null 2> "$CLIENT_ERR" &
client_pid=$!

# Wait until the query has been running for a second, i.e. it is inside the set-building pipeline rather
# than still being analyzed, and time out loudly instead of cancelling something else.
i=0
while [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$QUERY_ID' AND elapsed > 1")" -lt 1 ]; do
    sleep 0.3
    i=$((i + 1))
    if [ "$i" -gt 200 ]; then
        echo "Query did not start in time" >&2
        kill -9 $client_pid 2>/dev/null
        exit 1
    fi
done

# With `partial_result_on_first_cancel` the first cancel does not cancel the query through the process
# list: it only makes the interactive-cancel callback return true, which stops the nested set-build
# pipeline without an exception. Without the fix, the projection then evaluated a filter holding a set
# that was left unbuilt, and reported `Not-ready Set is passed as the second argument for function 'in'`.
kill -INT $client_pid
wait $client_pid

if grep -q -F "Not-ready Set" "$CLIENT_ERR"; then
    echo "FAIL: an unbuilt set reached the filter"
    cat "$CLIENT_ERR"
elif grep -q -F "cancelled while building a set for subquery" "$CLIENT_ERR"; then
    echo "the cancellation is reported"
else
    echo "FAIL: neither the cancellation nor the unbuilt set was reported"
    cat "$CLIENT_ERR"
fi

rm -f "$CLIENT_ERR"

# A nested `IN` makes the outer set source non-clonable because it contains a
# `DelayedCreatingSetsStep`. This takes the destructive ordered-set build path used for key analysis.
# The `EXPLAIN indexes` result proves that the predicate is used by the primary-key analysis; together
# with the pinned setting above, this makes it call `buildOrderedSetInplace` before the cancellation test.
$CLICKHOUSE_CLIENT -q "
    SELECT count() > 0 FROM (
        EXPLAIN indexes = 1
        SELECT count() FROM t_first_cancel_set_build WHERE a IN (
            SELECT number * 3 FROM numbers(3) WHERE sleep(0) = 0 AND number IN (SELECT number FROM numbers(3))
        ) SETTINGS $PINNED_SETTINGS
    ) WHERE explain ILIKE '%PrimaryKey%'"

QUERY_ID="${CLICKHOUSE_DATABASE}_first_cancel_ordered_set_build"
CLIENT_ERR="${CLICKHOUSE_TMP}/first_cancel_ordered_set_build.err"

# The slow stage must be the *outer* non-clonable set build itself, not the nested one: if the nested
# subquery were the long-running one, the cancellation would land in its own `buildSetInplace` and the
# unordered fix alone would report it, so this half would pass even with the ordered-path guard removed.
# The nested `IN` is therefore trivial (it only makes the source non-clonable) and `sleep` is the first
# conjunct, so it is evaluated for every block even under short-circuit evaluation.
$CLICKHOUSE_CLIENT --query_id="$QUERY_ID" -q "
    SELECT count() FROM t_first_cancel_set_build WHERE a IN (
        SELECT number * 3 FROM numbers(10000000)
        WHERE sleep(1) = 0 AND number IN (SELECT number FROM numbers(3))
    ) SETTINGS partial_result_on_first_cancel = 1, $PINNED_SETTINGS" > /dev/null 2> "$CLIENT_ERR" &
client_pid=$!

i=0
while [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$QUERY_ID' AND elapsed > 1")" -lt 1 ]; do
    sleep 0.3
    i=$((i + 1))
    if [ "$i" -gt 200 ]; then
        echo "Ordered-set query did not start in time" >&2
        kill -9 $client_pid 2>/dev/null
        exit 1
    fi
done

kill -INT $client_pid
wait $client_pid

if grep -q -F "Not-ready Set" "$CLIENT_ERR"; then
    echo "FAIL: an unbuilt ordered set reached the filter"
    cat "$CLIENT_ERR"
elif grep -q -F "cancelled while building an ordered set for subquery" "$CLIENT_ERR"; then
    echo "the ordered-set cancellation is reported"
else
    echo "FAIL: the ordered-set build did not report the cancellation"
    cat "$CLIENT_ERR"
fi

rm -f "$CLIENT_ERR"

# The cancellation must not leave the table unreadable, and a set for the same subquery shape must still
# be buildable afterwards.
$CLICKHOUSE_CLIENT -q "
    SELECT count(), sum(b) FROM t_first_cancel_set_build WHERE 1 IN (SELECT number FROM numbers(3))
    SETTINGS $PINNED_SETTINGS"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_first_cancel_set_build"
