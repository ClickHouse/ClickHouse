#!/usr/bin/env bash
# Regression test for issue #105501 (STID 1611-343c): `Trying to execute PLACEHOLDER action`.
#
# The AST fuzzer found that a `MATERIALIZED` CTE whose body references a column that
# does not exist in the inner FROM resolves that name against the outer scope, making
# the CTE accidentally correlated. Used as the RHS of `IN`, such a CTE produces a
# `FutureSetFromSubquery` whose source plan still contains `PLACEHOLDER` actions.
#
# `buildSetInplace` / `buildOrderedSetInplace` already short-circuit on plans with
# correlated expressions (see PR #100398). The companion `FutureSetFromSubquery::build`
# path used by `addCreatingSetsStep`, `addCreatingSetsTransform`, and
# `DelayedCreatingSetsStep::makePlansForSets` previously did not, so the optimizer
# could wrap the `PLACEHOLDER`-bearing plan in a `CreatingSetStep`, build a pipeline,
# and abort on the first chunk in `ExpressionActions::execute` with
# `LOGICAL_ERROR: 'Trying to execute PLACEHOLDER action'`.
#
# PR #105518 moves the `hasCorrelatedExpressions` guard inside `build` so every
# call site is covered.
#
# Current master rejects these patterns at analyzer level (`NOT_IMPLEMENTED` from
# `resolveFunction.cpp:997`, or `UNSUPPORTED_METHOD` when the implicit outer-scope
# resolution is itself disallowed), so the queries below never reach the planner
# today. This test pins that invariant: whatever happens, the output must NEVER
# contain `Trying to execute PLACEHOLDER action`. If the analyzer ever lets these
# patterns through (e.g. once decorrelation is implemented), the build-time guard
# remains as the last line of defence and the test still passes.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Run a query and assert it never reaches `Trying to execute PLACEHOLDER action`.
# A clean success (RC=0) is fine. A failure is fine ONLY if it is one of the
# analyzer/fuzzer rejections this test intentionally exercises; any other error
# (a setup failure, an unknown setting, "Not-ready Set is passed", etc.) is a real
# failure so the test does not silently pass when the boundary is never exercised.
assert_no_placeholder() {
    local desc="$1"
    local query="$2"
    local output rc
    output=$($CLICKHOUSE_CLIENT -q "$query" 2>&1)
    rc=$?

    if echo "$output" | grep -qF "Trying to execute PLACEHOLDER action"; then
        echo "FAIL ($desc): PLACEHOLDER action reached execution"
        echo "$output" >&2
        return 1
    fi

    if [ "$rc" -eq 0 ]; then
        echo "OK ($desc)"
        return 0
    fi

    if echo "$output" | grep -qE "NOT_IMPLEMENTED|UNSUPPORTED_METHOD|NUMBER_OF_COLUMNS_DOESNT_MATCH|UNKNOWN_IDENTIFIER|does not have valid source node"; then
        echo "OK ($desc)"
        return 0
    fi

    echo "FAIL ($desc): unexpected error"
    echo "$output" >&2
    return 1
}

# Case 1: the original AST fuzzer reproducer. `zeros(N)` returns column `zero`, so
# `SELECT number, number + 65535 FROM zeros(N)` makes the inner `number` references
# resolve against the outer `numbers(8).number` (correlated). The IN LHS is a
# 3-tuple while the CTE projects 2 columns, exposing both the correlation path and
# a tuple-arity mismatch.
assert_no_placeholder "fuzzer 3-tuple over zeros() CTE" "
SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;
WITH t AS MATERIALIZED (SELECT number, number + 65535 FROM zeros(8))
SELECT * FROM numbers(8) WHERE (number + 3, number + 1, number + 3) IN (t);
"

# Case 2: the simplified fuzzer pattern seen on master sha 6d9d7738 - single-column
# IN against a `MATERIALIZED` CTE over `primes()` (which exposes column `prime`),
# so `SELECT number FROM primes(N)` makes `number` a correlated outer reference.
assert_no_placeholder "fuzzer single-column over primes() CTE" "
SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;
WITH t AS MATERIALIZED (SELECT number FROM primes(5))
SELECT * FROM numbers(8) WHERE number IN (t);
"

# Case 3: the same pattern under the optimizer-pessimising settings that the fuzzer
# combined with the crash query. These knobs steer the plan through paths that
# rebuild sub-queries via `addCreatingSetsStep` / `DelayedCreatingSetsStep::makePlansForSets`
# rather than the inplace siblings, so they are the most direct exercise of the
# build-time guard.
assert_no_placeholder "fuzzer settings + primes() CTE" "
SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;
SET enable_join_runtime_filters = 0;
SET query_plan_merge_filter_into_join_condition = 0;
SET query_plan_merge_filters = 0;
SET query_plan_convert_any_join_to_semi_or_anti_join = 1;
WITH t AS MATERIALIZED (SELECT number FROM primes(5))
SELECT * FROM numbers(8) WHERE number IN (t);
"

# Case 4: explicit correlated subquery in IN (the canonical shape PR #100398 covered
# for `buildSetInplace` / `buildOrderedSetInplace`; the centralised guard in `build`
# now also covers the optimizer/mutation paths that reach `FutureSetFromSubquery::build`
# directly).
$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS t1_04274;
DROP TABLE IF EXISTS t2_04274;
CREATE TABLE t1_04274 (a UInt32, b UInt32) ENGINE = MergeTree() ORDER BY a;
CREATE TABLE t2_04274 (x UInt32, y UInt32) ENGINE = MergeTree() ORDER BY x;
INSERT INTO t1_04274 VALUES (1, 10), (2, 20), (3, 30);
INSERT INTO t2_04274 VALUES (10, 100), (20, 200), (40, 400);
"

assert_no_placeholder "explicit correlated IN subquery" "
SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;
SELECT a FROM t1_04274 WHERE a IN (SELECT x FROM t2_04274 WHERE t2_04274.y = t1_04274.b);
"

$CLICKHOUSE_CLIENT -q "
DROP TABLE t1_04274;
DROP TABLE t2_04274;
"
