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

CREATE TABLE jks2_left (user_id UInt64, request_id UInt64, payload UInt64)
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
OPTIMIZE TABLE jks2_right FINAL;

-- Blocker: the demotion gate only checked whether *some* mixed-capable algorithm is enabled,
-- but `chooseJoinAlgorithm` picks the first applicable one. With a merge algorithm listed
-- before hash, the merge join would be selected and silently ignore the demoted equality,
-- turning a two-key join into a one-key join with extra rows. Demotion must be skipped here.
SELECT 'merge_pref_inner' AS t,
    (SELECT count() FROM jks2_left l JOIN jks2_right r ON l.user_id = r.user_id AND l.request_id = r.request_id
        SETTINGS join_algorithm = 'full_sorting_merge,hash', query_plan_hash_join_subset_keys_auto = 1,
            query_plan_hash_join_subset_keys_min_rows = 0, query_plan_hash_join_subset_keys_min_kept_selectivity = 0.001)
  = (SELECT count() FROM jks2_left l JOIN jks2_right r ON l.user_id = r.user_id AND l.request_id = r.request_id
        SETTINGS join_algorithm = 'full_sorting_merge,hash', query_plan_hash_join_subset_keys_auto = 0) AS ok;

-- Blocker: demoted equalities are JOIN ON conditions, so on outer joins they must be evaluated
-- during the join (NULL-extending non-matching rows), not as a post-join filter that would drop
-- them. Verify outer-join results are unchanged by demotion.
SELECT 'left_outer' AS t,
    (SELECT count() FROM jks2_left l LEFT JOIN jks2_right r ON l.user_id = r.user_id AND l.request_id = r.request_id
        SETTINGS join_algorithm = 'hash', query_plan_hash_join_subset_keys_auto = 1,
            query_plan_hash_join_subset_keys_min_rows = 0, query_plan_hash_join_subset_keys_min_kept_selectivity = 0.001)
  = (SELECT count() FROM jks2_left l LEFT JOIN jks2_right r ON l.user_id = r.user_id AND l.request_id = r.request_id
        SETTINGS join_algorithm = 'hash', query_plan_hash_join_subset_keys_auto = 0) AS ok;

SELECT 'left_outer_nulls' AS t,
    (SELECT countIf(r.user_id IS NULL) FROM jks2_left l LEFT JOIN jks2_right r ON l.user_id = r.user_id AND l.request_id = r.request_id
        SETTINGS join_algorithm = 'hash', query_plan_hash_join_subset_keys_auto = 1,
            query_plan_hash_join_subset_keys_min_rows = 0, query_plan_hash_join_subset_keys_min_kept_selectivity = 0.001)
  = (SELECT countIf(r.user_id IS NULL) FROM jks2_left l LEFT JOIN jks2_right r ON l.user_id = r.user_id AND l.request_id = r.request_id
        SETTINGS join_algorithm = 'hash', query_plan_hash_join_subset_keys_auto = 0) AS ok;

SELECT 'full_outer' AS t,
    (SELECT count() FROM jks2_left l FULL JOIN jks2_right r ON l.user_id = r.user_id AND l.request_id = r.request_id
        SETTINGS join_algorithm = 'hash', query_plan_hash_join_subset_keys_auto = 1,
            query_plan_hash_join_subset_keys_min_rows = 0, query_plan_hash_join_subset_keys_min_kept_selectivity = 0.001)
  = (SELECT count() FROM jks2_left l FULL JOIN jks2_right r ON l.user_id = r.user_id AND l.request_id = r.request_id
        SETTINGS join_algorithm = 'hash', query_plan_hash_join_subset_keys_auto = 0) AS ok;

-- Outer join carrying an extra (non-equi) ON predicate alongside the demoted equality: the
-- extra condition must still be honored and results must match the non-demoted plan.
SELECT 'left_outer_extra_cond' AS t,
    (SELECT count() FROM jks2_left l LEFT JOIN jks2_right r
        ON l.user_id = r.user_id AND l.request_id = r.request_id AND r.extra < 500
        SETTINGS join_algorithm = 'hash', query_plan_hash_join_subset_keys_auto = 1,
            query_plan_hash_join_subset_keys_min_rows = 0, query_plan_hash_join_subset_keys_min_kept_selectivity = 0.001)
  = (SELECT count() FROM jks2_left l LEFT JOIN jks2_right r
        ON l.user_id = r.user_id AND l.request_id = r.request_id AND r.extra < 500
        SETTINGS join_algorithm = 'hash', query_plan_hash_join_subset_keys_auto = 0) AS ok;

DROP TABLE jks2_left;
DROP TABLE jks2_right;
