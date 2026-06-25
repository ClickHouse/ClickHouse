-- Tags: no-fasttest
-- Verify cardinality-driven JOIN key demotion: a high-NDV equality key is
-- moved from the hash table into the residual `mixed_join_expression` so the
-- hash table is built only on the low-NDV key(s) and stays compact.

SET allow_statistics = 1;
SET use_statistics = 1;
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET join_algorithm = 'hash';

DROP TABLE IF EXISTS jks_left;
DROP TABLE IF EXISTS jks_right;

CREATE TABLE jks_left (user_id UInt64, request_id UInt64, payload UInt64)
ENGINE = MergeTree ORDER BY user_id;

CREATE TABLE jks_right
(
    user_id UInt64 STATISTICS(uniq),
    request_id UInt64 STATISTICS(uniq),
    extra UInt64
)
ENGINE = MergeTree ORDER BY user_id;

INSERT INTO jks_left SELECT number % 500, number % 10, number FROM numbers(5000);
INSERT INTO jks_right SELECT number % 500, number % 10, number FROM numbers(5000);

OPTIMIZE TABLE jks_right FINAL;

-- Sanity: result count must not depend on the optimization (one matched pair per row).
SELECT 'count_on' AS label, count() FROM jks_left l JOIN jks_right r
    ON l.user_id = r.user_id AND l.request_id = r.request_id
SETTINGS query_plan_join_subset_keys_auto = 1,
    query_plan_join_subset_keys_min_rows = 0,
    query_plan_join_subset_keys_min_kept_selectivity = 0.001;

-- With the optimization off, both columns are hash keys.
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM jks_left l JOIN jks_right r
        ON l.user_id = r.user_id AND l.request_id = r.request_id
    SETTINGS query_plan_join_subset_keys_auto = 0
)
WHERE explain ILIKE '%Clauses%' OR explain ILIKE '%Residual filter%' OR explain ILIKE '%Mixed condition%';

-- With the optimization on and target_ndv = 5000 * 0.001 = 5, request_id (NDV=10) meets
-- the target and is the smallest single key that does, so user_id (NDV=500) is demoted.
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM jks_left l JOIN jks_right r
        ON l.user_id = r.user_id AND l.request_id = r.request_id
    SETTINGS query_plan_join_subset_keys_auto = 1,
    query_plan_join_subset_keys_min_rows = 0,
    query_plan_join_subset_keys_min_kept_selectivity = 0.001
)
WHERE explain ILIKE '%Clauses%' OR explain ILIKE '%Residual filter%' OR explain ILIKE '%Mixed condition%';

-- min_rows gate: right-side row count below the threshold disables the optimization.
SELECT 'rows_gate' AS label, trimLeft(explain) FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM jks_left l JOIN jks_right r
        ON l.user_id = r.user_id AND l.request_id = r.request_id
    SETTINGS query_plan_join_subset_keys_auto = 1, query_plan_join_subset_keys_min_rows = 100000
)
WHERE explain ILIKE '%Clauses%' OR explain ILIKE '%Residual filter%';

-- selectivity gate: target_ndv = 5000 * 2.0 = 10000, larger than any single-key NDV
-- (max is user_id with NDV=500). No key reaches the target -> optimization bails.
SELECT 'selectivity_gate' AS label, trimLeft(explain) FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM jks_left l JOIN jks_right r
        ON l.user_id = r.user_id AND l.request_id = r.request_id
    SETTINGS query_plan_join_subset_keys_auto = 1, query_plan_join_subset_keys_min_rows = 0,
    query_plan_join_subset_keys_min_kept_selectivity = 2.0
)
WHERE explain ILIKE '%Clauses%' OR explain ILIKE '%Residual filter%';

-- algorithm gate: `full_sorting_merge` does not accept mixed-condition predicates, so
-- demotion must be skipped (otherwise `chooseJoinAlgorithm` would throw). Plan must keep
-- both keys as equi keys.
SELECT 'algo_gate' AS label, trimLeft(explain) FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM jks_left l JOIN jks_right r
        ON l.user_id = r.user_id AND l.request_id = r.request_id
    SETTINGS join_algorithm = 'full_sorting_merge',
        query_plan_join_subset_keys_auto = 1,
        query_plan_join_subset_keys_min_rows = 0,
        query_plan_join_subset_keys_min_kept_selectivity = 0.001
)
WHERE explain ILIKE '%Clauses%' OR explain ILIKE '%Residual filter%' OR explain ILIKE '%Mixed condition%';

-- algorithm gate (`auto`): `chooseJoinAlgorithm` rejects `mixed_join_expression` when
-- `join_algorithm = 'auto'` even though the `JoinSwitcher` it builds wraps a `HashJoin`.
-- Demote must skip the rewrite so the query succeeds and the plan keeps both keys.
SELECT 'algo_gate_auto_count' AS label, count() FROM jks_left l JOIN jks_right r
    ON l.user_id = r.user_id AND l.request_id = r.request_id
SETTINGS join_algorithm = 'auto',
    query_plan_join_subset_keys_auto = 1,
    query_plan_join_subset_keys_min_rows = 0,
    query_plan_join_subset_keys_min_kept_selectivity = 0.001;

SELECT 'algo_gate_auto_plan' AS label, trimLeft(explain) FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM jks_left l JOIN jks_right r
        ON l.user_id = r.user_id AND l.request_id = r.request_id
    SETTINGS join_algorithm = 'auto',
        query_plan_join_subset_keys_auto = 1,
        query_plan_join_subset_keys_min_rows = 0,
        query_plan_join_subset_keys_min_kept_selectivity = 0.001
)
WHERE explain ILIKE '%Clauses%' OR explain ILIKE '%Residual filter%' OR explain ILIKE '%Mixed condition%';

-- strictness gate: `any_join_distinct_right_table_keys = 1` promotes `ANY` to `RightAny`,
-- which `HashJoin::validateAdditionalFilterExpression` rejects for mixed conditions. Demote
-- must skip such combinations so the query runs successfully and keeps both equi keys.
SELECT 'right_any_count' AS label, count() FROM jks_left l ANY LEFT JOIN jks_right r
    ON l.user_id = r.user_id AND l.request_id = r.request_id
SETTINGS any_join_distinct_right_table_keys = 1,
    query_plan_join_subset_keys_auto = 1,
    query_plan_join_subset_keys_min_rows = 0,
    query_plan_join_subset_keys_min_kept_selectivity = 0.001;

SELECT 'right_any_plan' AS label, trimLeft(explain) FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM jks_left l ANY LEFT JOIN jks_right r
        ON l.user_id = r.user_id AND l.request_id = r.request_id
    SETTINGS any_join_distinct_right_table_keys = 1,
        query_plan_join_subset_keys_auto = 1,
        query_plan_join_subset_keys_min_rows = 0,
        query_plan_join_subset_keys_min_kept_selectivity = 0.001
)
WHERE explain ILIKE '%Clauses%' OR explain ILIKE '%Residual filter%' OR explain ILIKE '%Mixed condition%';

-- conditional NDV: a `WHERE keep_col = const` makes the filtered stream's NDV(keep_col) = 1,
-- so even though full-table NDV(keep_col) > target_ndv, demote must not pick `keep_col` as
-- the kept hash key. The estimator should propagate NDV(keep_col)=1 into column_stats; demote
-- then picks `drop_col` (the real discriminator) or bails — both outcomes keep the residual
-- bucket bounded.
DROP TABLE IF EXISTS jks_right_filtered;
CREATE TABLE jks_right_filtered
(
    keep_col UInt64 STATISTICS(uniq),
    drop_col UInt64 STATISTICS(uniq),
    extra    UInt64
) ENGINE = MergeTree ORDER BY drop_col;

INSERT INTO jks_right_filtered SELECT intDiv(number, 100), number % 100, number FROM numbers(1000000);
OPTIMIZE TABLE jks_right_filtered FINAL;

SELECT 'filtered_picks_real_discriminator' AS label, trimLeft(explain) FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM jks_left l
    JOIN (SELECT * FROM jks_right_filtered WHERE keep_col = 5) r
        ON l.user_id = r.keep_col AND l.request_id = r.drop_col
    SETTINGS query_plan_join_subset_keys_auto = 1,
        query_plan_join_subset_keys_min_rows = 0,
        query_plan_join_subset_keys_min_kept_selectivity = 0.05
)
WHERE explain ILIKE '%Clauses%' OR explain ILIKE '%Residual filter%' OR explain ILIKE '%Mixed condition%';

DROP TABLE jks_right_filtered;

-- filter-aware profile: when the right side is wrapped in a WHERE that drops it below
-- `query_plan_join_subset_keys_min_rows`, demotion must bail. The raw table has 5000 rows
-- but the filter selects ~10% (~500 rows), below min_rows=4000.
SELECT 'filtered_below_min_rows' AS label, trimLeft(explain) FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM jks_left l JOIN (SELECT * FROM jks_right WHERE extra < 500) r
        ON l.user_id = r.user_id AND l.request_id = r.request_id
    SETTINGS query_plan_join_subset_keys_auto = 1,
        query_plan_join_subset_keys_min_rows = 4000,
        query_plan_join_subset_keys_min_kept_selectivity = 0.001
)
WHERE explain ILIKE '%Clauses%' OR explain ILIKE '%Residual filter%' OR explain ILIKE '%Mixed condition%';

DROP TABLE jks_left;
DROP TABLE jks_right;
