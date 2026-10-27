#!/usr/bin/env bash
# Regression test for the hasCorrelatedExpressions fix.
#
# Test 1 (SortingStep default):
# Before the fix, IQueryPlanStep::hasCorrelatedExpressions threw NOT_IMPLEMENTED
# for steps without an override (e.g. SortingStep). buildCorrelatedPlanStepMap
# called this on every plan step, so correlated subqueries whose plans contained
# SortingStep crashed with "Cannot check Sorting plan step for correlated expressions".
# After the fix, the default returns false and decorrelation produces a clearer error.
#
# Test 2 (IN-clause guard):
# A correlated subquery in an IN clause creates a FutureSetFromSubquery whose plan
# contains PLACEHOLDER nodes. Without the guard in buildSetInplace /
# buildOrderedSetInplace, executing this plan throws "Trying to execute PLACEHOLDER
# action". The fix detects correlated expressions and skips in-place set construction.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    SET enable_analyzer = 1;
    SET allow_experimental_correlated_subqueries = 1;

    DROP TABLE IF EXISTS t1;
    DROP TABLE IF EXISTS t2;

    CREATE TABLE t1 (a UInt32, b UInt32) ENGINE = MergeTree() ORDER BY a;
    CREATE TABLE t2 (x UInt32, y UInt32) ENGINE = MergeTree() ORDER BY x;

    INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 30);
    INSERT INTO t2 VALUES (10, 100), (20, 200), (40, 400);
"

# Test 1: Correlated scalar subquery with ORDER BY (SortingStep in the plan).
# On master: throws 'Cannot check Sorting plan step for correlated expressions'.
# After the fix, the default `hasCorrelatedExpressions` returns false instead of
# throwing `NOT_IMPLEMENTED`. The query may then either decorrelate successfully or
# fail with a clearer `NOT_IMPLEMENTED` message (e.g. `Cannot decorrelate query, ...`).
# Both are acceptable; any other outcome (including the pre-fix error or some new
# unexpected exception) must fail the test.
OUTPUT=$($CLICKHOUSE_CLIENT -q "
    SET enable_analyzer = 1;
    SET allow_experimental_correlated_subqueries = 1;
    SELECT a, (SELECT x FROM t2 WHERE t2.y = t1.a * 100 ORDER BY x) as s FROM t1 ORDER BY a;
" 2>&1)
RC=$?

if echo "$OUTPUT" | grep -q "Cannot check.*plan step for correlated expressions"; then
    echo "FAIL: got pre-fix error (hasCorrelatedExpressions threw instead of returning false)"
    echo "$OUTPUT" >&2
    exit 1
elif [ "$RC" -eq 0 ] || echo "$OUTPUT" | grep -qE "Code: 48|NOT_IMPLEMENTED|Cannot decorrelate"; then
    echo "OK: did not hit pre-fix hasCorrelatedExpressions failure"
else
    echo "FAIL: unexpected output (rc=$RC)"
    echo "$OUTPUT" >&2
    exit 1
fi

# Test 2: Correlated subquery in an IN clause.
# The subquery plan contains `PLACEHOLDER` nodes for the correlated column (`t1.b`).
# Without the fix, `buildSetInplace` / `buildOrderedSetInplace` would attempt to execute
# this plan and throw `Trying to execute PLACEHOLDER action`.
# With the fix, in-place set construction is skipped for correlated plans, so the query
# must either succeed (with the expected empty result for this data set, since no `t2.y`
# matches any `t1.b`) or fail with a clear `NOT_IMPLEMENTED` decorrelation error.
# Any other outcome must fail the test.
OUTPUT=$($CLICKHOUSE_CLIENT -q "
    SET enable_analyzer = 1;
    SET allow_experimental_correlated_subqueries = 1;
    SELECT a FROM t1 WHERE a IN (SELECT x FROM t2 WHERE t2.y = t1.b);
" 2>&1)
RC=$?

if echo "$OUTPUT" | grep -q "Trying to execute PLACEHOLDER action"; then
    echo "FAIL: correlated IN subquery triggered PLACEHOLDER execution"
    echo "$OUTPUT" >&2
    exit 1
elif [ "$RC" -eq 0 ]; then
    # Success path: for this data set no row of t1.b matches any t2.y, so the result
    # must be empty. A non-empty result here would be a semantic regression.
    TRIMMED=$(echo "$OUTPUT" | tr -d '[:space:]')
    if [ -z "$TRIMMED" ]; then
        echo "OK: correlated IN subquery did not trigger PLACEHOLDER execution"
    else
        echo "FAIL: correlated IN subquery returned unexpected rows"
        echo "$OUTPUT" >&2
        exit 1
    fi
elif echo "$OUTPUT" | grep -qE "Code: 48|NOT_IMPLEMENTED|Cannot decorrelate"; then
    echo "OK: correlated IN subquery did not trigger PLACEHOLDER execution"
else
    echo "FAIL: unexpected output (rc=$RC)"
    echo "$OUTPUT" >&2
    exit 1
fi

$CLICKHOUSE_CLIENT -q "
    DROP TABLE t1;
    DROP TABLE t2;
"
