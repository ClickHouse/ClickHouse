-- Tags: no-fasttest
-- Correctness regressions for cardinality-driven JOIN key demotion
-- (`query_plan_hash_join_subset_keys_auto`). Each case asserts that turning the optimization
-- on produces exactly the same result as turning it off - demotion must never change results.
-- These are result-based (not `EXPLAIN`-based) so they run under randomized settings.
-- Kept small so it stays well under the flaky-check per-run time limit.

SET allow_statistics = 1;
SET use_statistics = 1;
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;

DROP TABLE IF EXISTS jks2_left;
DROP TABLE IF EXISTS jks2_right;

-- Both sides carry statistics: which one the join-order optimizer picks as the build side is not
-- fixed by `query_plan_join_swap_table` alone, and a build side without column statistics has no
-- NDVs for the demotion to score, so the plan-shape assertions below would fail whenever the
-- orientation flipped.
CREATE TABLE jks2_left
(
    user_id UInt64 STATISTICS(uniq),
    request_id UInt64 STATISTICS(uniq),
    payload UInt64
)
ENGINE = MergeTree ORDER BY user_id;

CREATE TABLE jks2_right
(
    user_id UInt64 STATISTICS(uniq),
    request_id UInt64 STATISTICS(uniq),
    extra UInt64
)
ENGINE = MergeTree ORDER BY user_id;

-- Left keys 0..119; right keys 0..99, so left rows with user_id >= 100 have no match and
-- exercise outer-join NULL-extension. `request_id` (NDV 10) is the low-NDV key demotion keeps.
INSERT INTO jks2_left SELECT number % 120, number % 10, number FROM numbers(1200);
INSERT INTO jks2_right SELECT number % 100, number % 10, number FROM numbers(1000);
OPTIMIZE TABLE jks2_left FINAL;
OPTIMIZE TABLE jks2_right FINAL;

-- Blocker: the demotion gate only checked whether *some* mixed-capable algorithm is enabled,
-- but `chooseJoinAlgorithm` picks the first applicable one. With a merge algorithm listed
-- before hash, the merge join would be selected and silently ignore the demoted equality,
-- turning a two-key join into a one-key join with extra rows. Demotion must be skipped here.
SELECT 'merge_pref_inner' AS t,
    (SELECT count() FROM jks2_left l JOIN jks2_right r ON l.user_id = r.user_id AND l.request_id = r.request_id
        SETTINGS join_algorithm = 'full_sorting_merge,hash', query_plan_hash_join_subset_keys_auto = 1,
            query_plan_hash_join_subset_keys_min_rows = 0, query_plan_hash_join_subset_keys_min_kept_selectivity = 0.001,
            query_plan_hash_join_subset_keys_max_probe_cost_ns = 1e9, query_plan_hash_join_subset_keys_min_saving_bytes = 0)
  = (SELECT count() FROM jks2_left l JOIN jks2_right r ON l.user_id = r.user_id AND l.request_id = r.request_id
        SETTINGS join_algorithm = 'full_sorting_merge,hash', query_plan_hash_join_subset_keys_auto = 0) AS ok;

-- `parallel_full_sorting_merge` is likewise blind to a mixed condition, and unlike the merge
-- algorithms above it was added after the demotion gate was first written - the gate is an
-- allowlist so that a newly added algorithm is excluded until it is known to evaluate one.
SELECT 'parallel_merge_pref_inner' AS t,
    (SELECT count() FROM jks2_left l JOIN jks2_right r ON l.user_id = r.user_id AND l.request_id = r.request_id
        SETTINGS join_algorithm = 'parallel_full_sorting_merge,hash', query_plan_hash_join_subset_keys_auto = 1,
            query_plan_hash_join_subset_keys_min_rows = 0, query_plan_hash_join_subset_keys_min_kept_selectivity = 0.001,
            query_plan_hash_join_subset_keys_max_probe_cost_ns = 1e9, query_plan_hash_join_subset_keys_min_saving_bytes = 0)
  = (SELECT count() FROM jks2_left l JOIN jks2_right r ON l.user_id = r.user_id AND l.request_id = r.request_id
        SETTINGS join_algorithm = 'parallel_full_sorting_merge,hash', query_plan_hash_join_subset_keys_auto = 0) AS ok;

-- Blocker: demoted equalities are JOIN ON conditions, so on outer joins they must be evaluated
-- during the join (NULL-extending non-matching rows), not as a post-join filter that would drop
-- them. Verify outer-join results are unchanged by demotion.
SELECT 'left_outer' AS t,
    (SELECT count() FROM jks2_left l LEFT JOIN jks2_right r ON l.user_id = r.user_id AND l.request_id = r.request_id
        SETTINGS join_algorithm = 'hash', query_plan_hash_join_subset_keys_auto = 1,
            query_plan_hash_join_subset_keys_min_rows = 0, query_plan_hash_join_subset_keys_min_kept_selectivity = 0.001,
            query_plan_hash_join_subset_keys_max_probe_cost_ns = 1e9, query_plan_hash_join_subset_keys_min_saving_bytes = 0)
  = (SELECT count() FROM jks2_left l LEFT JOIN jks2_right r ON l.user_id = r.user_id AND l.request_id = r.request_id
        SETTINGS join_algorithm = 'hash', query_plan_hash_join_subset_keys_auto = 0) AS ok;

SELECT 'left_outer_nulls' AS t,
    (SELECT countIf(r.user_id IS NULL) FROM jks2_left l LEFT JOIN jks2_right r ON l.user_id = r.user_id AND l.request_id = r.request_id
        SETTINGS join_algorithm = 'hash', query_plan_hash_join_subset_keys_auto = 1,
            query_plan_hash_join_subset_keys_min_rows = 0, query_plan_hash_join_subset_keys_min_kept_selectivity = 0.001,
            query_plan_hash_join_subset_keys_max_probe_cost_ns = 1e9, query_plan_hash_join_subset_keys_min_saving_bytes = 0)
  = (SELECT countIf(r.user_id IS NULL) FROM jks2_left l LEFT JOIN jks2_right r ON l.user_id = r.user_id AND l.request_id = r.request_id
        SETTINGS join_algorithm = 'hash', query_plan_hash_join_subset_keys_auto = 0) AS ok;

SELECT 'full_outer' AS t,
    (SELECT count() FROM jks2_left l FULL JOIN jks2_right r ON l.user_id = r.user_id AND l.request_id = r.request_id
        SETTINGS join_algorithm = 'hash', query_plan_hash_join_subset_keys_auto = 1,
            query_plan_hash_join_subset_keys_min_rows = 0, query_plan_hash_join_subset_keys_min_kept_selectivity = 0.001,
            query_plan_hash_join_subset_keys_max_probe_cost_ns = 1e9, query_plan_hash_join_subset_keys_min_saving_bytes = 0)
  = (SELECT count() FROM jks2_left l FULL JOIN jks2_right r ON l.user_id = r.user_id AND l.request_id = r.request_id
        SETTINGS join_algorithm = 'hash', query_plan_hash_join_subset_keys_auto = 0) AS ok;

-- Outer join carrying an extra (non-equi) ON predicate alongside the demoted equality: the
-- extra condition must still be honored and results must match the non-demoted plan.
SELECT 'left_outer_extra_cond' AS t,
    (SELECT count() FROM jks2_left l LEFT JOIN jks2_right r
        ON l.user_id = r.user_id AND l.request_id = r.request_id AND r.extra < 500
        SETTINGS join_algorithm = 'hash', query_plan_hash_join_subset_keys_auto = 1,
            query_plan_hash_join_subset_keys_min_rows = 0, query_plan_hash_join_subset_keys_min_kept_selectivity = 0.001,
            query_plan_hash_join_subset_keys_max_probe_cost_ns = 1e9, query_plan_hash_join_subset_keys_min_saving_bytes = 0)
  = (SELECT count() FROM jks2_left l LEFT JOIN jks2_right r
        ON l.user_id = r.user_id AND l.request_id = r.request_id AND r.extra < 500
        SETTINGS join_algorithm = 'hash', query_plan_hash_join_subset_keys_auto = 0) AS ok;

-- Unlike the result comparisons above, the two checks below assert the SHAPE of the plan, so the
-- inputs to the join-order decision have to be pinned. The demotion only exists inside
-- `chooseJoinOrder`, so reordering has to be enabled at all - the harness sets
-- `query_plan_optimize_join_order_limit` to 0 or 1 often enough to matter, and either is too low
-- to reorder a two-table join. It also randomizes the order itself
-- (`query_plan_optimize_join_order_randomize`), which moves the build side.
--
-- Every case above compares a demoted plan against a non-demoted one, so all of them pass
-- whether or not the optimization actually fired. Assert that it does fire on this fixture,
-- otherwise the cases silently stop testing anything - which is how the perf test for this
-- feature went unnoticed while measuring nothing at all.
SELECT 'demote_fired' AS t, countIf(explain LIKE '%Residual filter%') > 0 AS ok
FROM (
    EXPLAIN actions = 1
    SELECT count() FROM jks2_left l JOIN jks2_right r
        ON l.user_id = r.user_id AND l.request_id = r.request_id
    SETTINGS join_algorithm = 'hash', query_plan_hash_join_subset_keys_auto = 1,
        query_plan_optimize_join_order_randomize = 0, query_plan_join_swap_table = 'false',
        query_plan_optimize_join_order_limit = 10,
        allow_statistics = 1, use_statistics = 1,
        query_plan_hash_join_subset_keys_min_rows = 0,
        query_plan_hash_join_subset_keys_min_kept_selectivity = 0.001,
        query_plan_hash_join_subset_keys_max_probe_cost_ns = 1e9,
        query_plan_hash_join_subset_keys_min_saving_bytes = 0
);

-- With the default gate the same fixture must be REFUSED. Here it is the saving that fails:
-- keying on `user_id` alone would hold ~256 cells instead of ~2048, tens of kilobytes, far below
-- `query_plan_hash_join_subset_keys_min_saving_bytes`. Paying any probe-time work for that is a
-- loss, and on a fixture this small the whole hash table is noise against the query's memory.
SELECT 'default_gate_declines' AS t, countIf(explain LIKE '%Residual filter%') = 0 AS ok
FROM (
    EXPLAIN actions = 1
    SELECT count() FROM jks2_left l JOIN jks2_right r
        ON l.user_id = r.user_id AND l.request_id = r.request_id
    SETTINGS join_algorithm = 'hash', query_plan_hash_join_subset_keys_auto = 1,
        query_plan_optimize_join_order_randomize = 0, query_plan_join_swap_table = 'false',
        query_plan_optimize_join_order_limit = 10,
        allow_statistics = 1, use_statistics = 1,
        query_plan_hash_join_subset_keys_min_rows = 0,
        query_plan_hash_join_subset_keys_min_kept_selectivity = 0.001
);

DROP TABLE jks2_left;
DROP TABLE jks2_right;
