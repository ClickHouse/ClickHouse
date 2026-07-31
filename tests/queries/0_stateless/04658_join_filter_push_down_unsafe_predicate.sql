-- { echo }
-- The mechanism is analyzer-only.
SET enable_analyzer = 1;
-- The fabricated-NULL oracle below needs the NULLs to be observable.
SET join_use_nulls = 1;
-- Filter text is printed only under `actions`, and the stress runner randomizes
-- `compatibility`, which reverts the default of this setting.
SET explain_query_plan_default = 'pretty';
-- aiEmbed is the only live function that is stateful AND deterministic in query scope.
-- It is referenced from EXPLAIN only, so it is never executed and contacts nothing.
SET allow_experimental_ai_functions = 1;

DROP TABLE IF EXISTS t_left;
DROP TABLE IF EXISTS t_right;
DROP TABLE IF EXISTS t_left_big;
DROP TABLE IF EXISTS t_right_big;

CREATE TABLE t_left (a UInt64, s String) ENGINE = Log;
CREATE TABLE t_right (a UInt64, b UInt64) ENGINE = Log;
INSERT INTO t_left SELECT number, toString(number) FROM numbers(2000);
INSERT INTO t_right SELECT number, number FROM numbers(2000);

CREATE TABLE t_left_big (a UInt64) ENGINE = Log;
CREATE TABLE t_right_big (a UInt64, b UInt64) ENGINE = Log;
INSERT INTO t_left_big SELECT number FROM numbers(200000);
INSERT INTO t_right_big SELECT number, number FROM numbers(200000);

-- Oracle A: fabricated NULLs.
-- The predicate mentions only the left (preserved) side, so every row that survives a
-- POST-join filter is a matched row and t_right.b can never be NULL. A nonzero count means
-- the predicate was evaluated on the right side before the join, deleted rows there, and the
-- fabricated NULLs then escaped a post-join filter the push-down had emptied.
-- Correct answer is exactly 0, on every run.

-- These rows observe the main path's MOVE defect only. Oracle A is blind to the partial path's
-- COPY defect, because the partial filter lands on the preserved left side and a copied predicate
-- still constrains the post-join rows, so no NULL is fabricated; B1 and B3 observe the copy.
-- Run at the shipped default of use_join_disjunctions_push_down.
-- Pre-fix this printed 0 (the count was about 500).
SELECT 'A1 default', countIf(rb IS NULL) = 0 FROM (
    SELECT t_right.b AS rb FROM t_left LEFT JOIN t_right ON t_left.a = t_right.a
    WHERE arrayExists(z -> rand(z) % 2 = 0, [t_left.a])
) SETTINGS query_plan_filter_push_down = 1;

-- Both values of use_join_disjunctions_push_down, so the main path is observed with the partial
-- path enabled and disabled. Pre-fix both printed 0 (counts about 500).
SELECT 'A2 disjunctions on', countIf(rb IS NULL) = 0 FROM (
    SELECT t_right.b AS rb FROM t_left LEFT JOIN t_right ON t_left.a = t_right.a
    WHERE arrayExists(z -> rand(z) % 2 = 0, [t_left.a])
) SETTINGS query_plan_filter_push_down = 1, use_join_disjunctions_push_down = 1;

SELECT 'A3 disjunctions off', countIf(rb IS NULL) = 0 FROM (
    SELECT t_right.b AS rb FROM t_left LEFT JOIN t_right ON t_left.a = t_right.a
    WHERE arrayExists(z -> rand(z) % 2 = 0, [t_left.a])
) SETTINGS query_plan_filter_push_down = 1, use_join_disjunctions_push_down = 0;

-- Push-down disabled: 0 on a buggy build too, so the rows above are not merely detecting
-- that the optimization ran.
SELECT 'A4 push down off', countIf(rb IS NULL) = 0 FROM (
    SELECT t_right.b AS rb FROM t_left LEFT JOIN t_right ON t_left.a = t_right.a
    WHERE arrayExists(z -> rand(z) % 2 = 0, [t_left.a])
) SETTINGS query_plan_filter_push_down = 0;

-- A deterministic lambda body: 0 on a buggy build too, so the rows above are not a blanket
-- veto on lambdas.
SELECT 'A5 deterministic lambda', countIf(rb IS NULL) = 0 FROM (
    SELECT t_right.b AS rb FROM t_left LEFT JOIN t_right ON t_left.a = t_right.a
    WHERE arrayExists(z -> z % 2 = 0, [t_left.a])
) SETTINGS query_plan_filter_push_down = 1;

-- A nested lambda: the unsafe call is in the inner body, which reaches the outer node only
-- through the captured columns of the outer lambda. Pre-fix this printed 0.
SELECT 'A6 nested lambda', countIf(rb IS NULL) = 0 FROM (
    SELECT t_right.b AS rb FROM t_left LEFT JOIN t_right ON t_left.a = t_right.a
    WHERE arrayExists(y -> arrayExists(z -> rand(z) % 2 = 0, [y]), [t_left.a])
) SETTINGS query_plan_filter_push_down = 1;

-- When the unsafe call references an outer column or takes no argument, the analyzer hoists it
-- out of the lambda into a visible node, where the pre-existing check already caught it.
-- 0 on both builds.
SELECT 'A7 hoisted outer column', countIf(rb IS NULL) = 0 FROM (
    SELECT t_right.b AS rb FROM t_left LEFT JOIN t_right ON t_left.a = t_right.a
    WHERE arrayExists(z -> rand(t_left.a) % 2 = 0, [t_left.a])
) SETTINGS query_plan_filter_push_down = 1;

SELECT 'A8 hoisted no argument', countIf(rb IS NULL) = 0 FROM (
    SELECT t_right.b AS rb FROM t_left LEFT JOIN t_right ON t_left.a = t_right.a
    WHERE arrayExists(z -> rand() % 2 = 0, [t_left.a])
) SETTINGS query_plan_filter_push_down = 1;

-- A capturing lambda. The rows above all mix the unsafe call with the lambda PARAMETER only, so
-- the lambda folds to a constant ColumnFunction and is reached through the node's column. Mixing
-- the parameter with an outer column instead leaves a genuine FunctionCapture on the node, whose
-- body DAG has to be entered through a different code path. Pre-fix this printed 0.
SELECT 'A9 capturing lambda', countIf(rb IS NULL) = 0 FROM (
    SELECT t_right.b AS rb FROM t_left LEFT JOIN t_right ON t_left.a = t_right.a
    WHERE arrayExists(z -> rand(z + t_left.a) % 2 = 0, [t_left.a])
) SETTINGS query_plan_filter_push_down = 1;

-- Oracle B: surviving row count.
-- The partial push-down path copies the conjunct below the join and keeps it above, so a
-- non-deterministic predicate is drawn twice and about a quarter of the rows survive instead
-- of about a half. Assert the two arms agree within 5%: pre-fix the ratio is about 0.5,
-- post-fix about 1.0. Oracle A cannot see this defect, because a copied predicate still
-- constrains the post-join rows and fabricates no NULLs.
-- This shape needs no lambda at all: the partial path had no determinism check whatsoever.
-- This is the only row that observes the partial path's own-metadata determinism rejection: the
-- main path rejects a visible non-deterministic function node on master already, so this verdict
-- can move only via the partial path, which is gated on use_join_disjunctions_push_down. The
-- runner randomizes that setting off directly and the stress runner's randomized `compatibility`
-- reverts its pre-26.1 default, so pin it or the row reads 1 whatever the optimizer does.
SELECT 'B1 visible non deterministic', abs((
    (SELECT count() FROM (
        SELECT t_left_big.a FROM t_left_big LEFT JOIN t_right_big ON t_left_big.a = t_right_big.a
        WHERE rand(t_left_big.a) % 2 = 0) SETTINGS
            query_plan_filter_push_down = 1, use_join_disjunctions_push_down = 1) /
    (SELECT count() FROM (
        SELECT t_left_big.a FROM t_left_big LEFT JOIN t_right_big ON t_left_big.a = t_right_big.a
        WHERE rand(t_left_big.a) % 2 = 0) SETTINGS query_plan_filter_push_down = 0)) - 1.) < 0.05;

-- A deterministic predicate must keep the same count with and without push-down.
SELECT 'B2 visible deterministic', abs((
    (SELECT count() FROM (
        SELECT t_left_big.a FROM t_left_big LEFT JOIN t_right_big ON t_left_big.a = t_right_big.a
        WHERE t_left_big.a % 2 = 0) SETTINGS query_plan_filter_push_down = 1) /
    (SELECT count() FROM (
        SELECT t_left_big.a FROM t_left_big LEFT JOIN t_right_big ON t_left_big.a = t_right_big.a
        WHERE t_left_big.a % 2 = 0) SETTINGS query_plan_filter_push_down = 0)) - 1.) < 0.05;

-- The same count oracle with the non-deterministic call HIDDEN in a lambda body. Neither row
-- above can observe the partial path's lambda blindness: A2 uses the hidden lambda but the NULL
-- oracle, which is blind to a copy (a copied predicate still constrains the post-join rows, and
-- the partial filter lands on the preserved left side, so no NULL is fabricated), while B1 uses
-- this oracle but a visible `rand`, which the node's own metadata already rejects. The partial
-- path is on by default, but the runner randomizes the setting off directly and the stress
-- runner's randomized `compatibility` reverts it below 26.1, so pin it: this row observes only
-- that path, and with it off the row reads 1 whatever the optimizer does.
SELECT 'B3 hidden non deterministic', abs((
    (SELECT count() FROM (
        SELECT t_left_big.a FROM t_left_big LEFT JOIN t_right_big ON t_left_big.a = t_right_big.a
        WHERE arrayExists(z -> rand(z) % 2 = 0, [t_left_big.a])) SETTINGS
            query_plan_filter_push_down = 1, use_join_disjunctions_push_down = 1) /
    (SELECT count() FROM (
        SELECT t_left_big.a FROM t_left_big LEFT JOIN t_right_big ON t_left_big.a = t_right_big.a
        WHERE arrayExists(z -> rand(z) % 2 = 0, [t_left_big.a])) SETTINGS
            query_plan_filter_push_down = 0)) - 1.) < 0.05;

-- Plan shape, for the carriers no execution oracle reaches.
-- A pushed filter appears AFTER the Join in the top-down plan dump, so a Filter row whose
-- index exceeds the Join's index is a filter sitting below the join.
-- Two settings would otherwise reorder the plan and add a BuildRuntimeFilter step with its own
-- Filter below the join, which this row-order oracle would count as a pushed filter. Both are
-- randomized by the test runners, so pin them: 'true' forces a join swap, and a nonzero
-- randomize seed replaces the join-order statistics, which forces the swap indirectly.
SET query_plan_join_swap_table = 'auto';
SET query_plan_optimize_join_order_randomize = 0;

-- A stateful function is vetoed for a whole plan step, not per conjunct, so nothing is pushed:
-- neither the stateful conjunct nor its deterministic sibling. Preserving that granularity is
-- what keeps the row set the stateful function sees unchanged. 0 on both builds.
SELECT 'P1 visible stateful step level', countIf(is_filter AND rn > join_rn) = 0 FROM (
    SELECT explain ILIKE '%Filter%' AS is_filter, rowNumberInAllBlocks() AS rn,
           max(if(explain ILIKE '%Join (JOIN%', rowNumberInAllBlocks(), 0)) OVER () AS join_rn
    FROM (
        EXPLAIN SELECT t_left.a FROM t_left LEFT JOIN t_right ON t_left.a = t_right.a
        WHERE rowNumberInAllBlocks() < 5 AND t_left.a % 2 = 0
    ) SETTINGS query_plan_filter_push_down = 1
);

-- A stateful call hidden in a lambda body must be vetoed the same way. aiEmbed is stateful yet
-- deterministic in query scope, so only the statefulness check can reject it.
-- Pre-fix this printed 0 (two Filter rows sat below the Join).
SELECT 'P2 hidden stateful', countIf(is_filter AND rn > join_rn) = 0 FROM (
    SELECT explain ILIKE '%Filter%' AS is_filter, rowNumberInAllBlocks() AS rn,
           max(if(explain ILIKE '%Join (JOIN%', rowNumberInAllBlocks(), 0)) OVER () AS join_rn
    FROM (
        EXPLAIN SELECT t_left.a FROM t_left LEFT JOIN t_right ON t_left.a = t_right.a
        WHERE arrayExists(z -> length(aiEmbed(z, 'm')) > 0, [t_left.s])
    ) SETTINGS query_plan_filter_push_down = 1
);

-- The statefulness veto must stay step-level even when the stateful call is hidden, so a
-- deterministic SIBLING conjunct must not be pushed either. Vetoing statefulness per conjunct
-- instead would let the sibling move and hand the stateful function a different row set:
-- a behaviour change smuggled beside a correctness fix. Pre-fix this printed 0.
SELECT 'P3 hidden stateful sibling', countIf(is_filter AND rn > join_rn) = 0 FROM (
    SELECT explain ILIKE '%Filter%' AS is_filter, rowNumberInAllBlocks() AS rn,
           max(if(explain ILIKE '%Join (JOIN%', rowNumberInAllBlocks(), 0)) OVER () AS join_rn
    FROM (
        EXPLAIN SELECT t_left.a FROM t_left LEFT JOIN t_right ON t_left.a = t_right.a
        WHERE arrayExists(z -> length(aiEmbed(z, 'm')) > 0, [t_left.s]) AND t_left.a % 2 = 0
    ) SETTINGS query_plan_filter_push_down = 1
);

-- The optimization must survive: a deterministic lambda is still pushed below the join.
-- Without this row the fix could degrade into refusing to push any lambda at all.
SELECT 'P4 deterministic lambda still pushed', countIf(is_filter AND rn > join_rn) > 0 FROM (
    SELECT explain ILIKE '%Filter%' AS is_filter, rowNumberInAllBlocks() AS rn,
           max(if(explain ILIKE '%Join (JOIN%', rowNumberInAllBlocks(), 0)) OVER () AS join_rn
    FROM (
        EXPLAIN SELECT t_left.a FROM t_left LEFT JOIN t_right ON t_left.a = t_right.a
        WHERE arrayExists(z -> z % 2 = 0, [t_left.a])
    ) SETTINGS query_plan_filter_push_down = 1
);

-- A deterministic non-lambda predicate is still pushed below the join.
SELECT 'P5 deterministic predicate still pushed', countIf(is_filter AND rn > join_rn) > 0 FROM (
    SELECT explain ILIKE '%Filter%' AS is_filter, rowNumberInAllBlocks() AS rn,
           max(if(explain ILIKE '%Join (JOIN%', rowNumberInAllBlocks(), 0)) OVER () AS join_rn
    FROM (
        EXPLAIN SELECT t_left.a FROM t_left LEFT JOIN t_right ON t_left.a = t_right.a
        WHERE t_left.a % 2 = 0
    ) SETTINGS query_plan_filter_push_down = 1
);

-- Leaving the predicate above the JOIN hands it to the next optimization, which decides whether a
-- LEFT JOIN can become an INNER one by evaluating the filter on a single fabricated not-matched row.
-- That verdict is only sound for a predicate whose value does not vary per row, so the same lambda
-- blindness has to be closed there too: below, the `t_right.b > 1` atom alone would license the
-- rewrite, and the hidden non-deterministic call must veto it. The join kind must stay LEFT.
-- The runner turns the rewrite off in about 5% of runs and the stress runner's randomized
-- `compatibility` reverts its pre-24.4 default, so pin it or the row passes without testing anything.
SELECT 'J1 hidden non deterministic keeps join kind', countIf(explain ILIKE '%Type: left%') = 1 FROM (
    EXPLAIN SELECT t_left.a FROM t_left LEFT JOIN t_right ON t_left.a = t_right.a
    WHERE arrayExists(z -> rand(z) % 2 = 0, [t_left.a]) AND t_right.b > 1
) SETTINGS query_plan_filter_push_down = 1, query_plan_convert_outer_join_to_inner_join = 1;

-- The same shape with the call visible: the rewrite is already vetoed on master, so this row does
-- not move. It pins the asymmetry that makes J1 a consequence of leaving the predicate above the
-- join rather than a pre-existing gap.
SELECT 'J2 visible non deterministic keeps join kind', countIf(explain ILIKE '%Type: left%') = 1 FROM (
    EXPLAIN SELECT t_left.a FROM t_left LEFT JOIN t_right ON t_left.a = t_right.a
    WHERE rand(t_left.a) % 2 = 0 AND t_right.b > 1
) SETTINGS query_plan_filter_push_down = 1, query_plan_convert_outer_join_to_inner_join = 1;

-- The rewrite must survive: a deterministic lambda body still lets the join kind change.
-- Without this row the veto could degrade into refusing to convert any lambda-bearing filter.
SELECT 'J3 deterministic lambda still converts', countIf(explain ILIKE '%Type: left%') = 0 FROM (
    EXPLAIN SELECT t_left.a FROM t_left LEFT JOIN t_right ON t_left.a = t_right.a
    WHERE arrayExists(z -> z % 2 = 0, [t_left.a]) AND t_right.b > 1
) SETTINGS query_plan_filter_push_down = 1, query_plan_convert_outer_join_to_inner_join = 1;

DROP TABLE t_left;
DROP TABLE t_right;
DROP TABLE t_left_big;
DROP TABLE t_right_big;
