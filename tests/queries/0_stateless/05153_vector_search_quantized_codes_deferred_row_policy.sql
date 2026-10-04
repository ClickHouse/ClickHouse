-- Tags: no-fasttest, no-ordinary-database, no-parallel-replicas
-- no-parallel-replicas: the two-stage quantized-codes rewrite is deliberately disabled under
-- parallel replicas, so the plan-shape assertion below cannot hold there.

-- `ReadFromMergeTree::addReadColumn` pulls the `vec.quantized` companion subcolumn into the read
-- after query analysis, so the subcolumn has to be passed through every filter `ActionsDAG` that
-- the read replays - otherwise it is dropped before the shortlist expression can rank on it. With
-- `FINAL` the row policy is not applied during reading but replayed by `FilterTransform` after
-- merging, from a separate deferred carrier, and that carrier needs the passthrough as well.

DROP ROW POLICY IF EXISTS 05153_vector_row_policy ON tab_vec_quantized_row_policy;
DROP TABLE IF EXISTS tab_vec_quantized_row_policy;

SET enable_quantized_codec = 1;
SET vector_search_use_quantized_codes = 1;
SET vector_search_index_fetch_multiplier = 100;
-- The shortlist size is clamped to `query_plan_max_limit_for_lazy_materialization`, and the plan
-- shape the rewrite needs is produced by lazy materialization; both are randomized by the harness.
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 1000000;
-- The shortlist uses internal functions that a remote worker cannot deserialize, so the rewrite
-- bails out for a distributed plan, which the `distributed plan` job enables globally.
SET make_distributed_plan = 0;
-- Deferring the row policy after `FINAL` is what splits it into its own carrier.
SET apply_row_policy_after_final = 1;

CREATE TABLE tab_vec_quantized_row_policy
(
    id UInt32,
    tenant UInt8,
    vec Array(Float32) CODEC(Quantized('int8', 8))
)
ENGINE = ReplacingMergeTree ORDER BY id;

INSERT INTO tab_vec_quantized_row_policy
SELECT number, number % 2, arrayMap(j -> toFloat32(sipHash64(number, j) % 100), range(8))
FROM numbers(500);

-- `tenant` is not a part of the sorting key, so the policy verdict is not the same for every row of
-- a deduplication group and it cannot be applied before `FINAL` merging: it is deferred.
CREATE ROW POLICY 05153_vector_row_policy ON tab_vec_quantized_row_policy
USING tenant = 1 AS RESTRICTIVE TO ALL;

-- The plan really contains the quantized shortlist, so the check below is not vacuous.
SELECT 'plan_has_shortlist_under_deferred_row_policy',
    countIf(explain ILIKE '%quantized shortlist%') > 0
FROM
(
    EXPLAIN PLAN
    SELECT id FROM tab_vec_quantized_row_policy FINAL
    ORDER BY L2Distance(vec, (SELECT vec FROM tab_vec_quantized_row_policy WHERE id = 43)) ASC
    LIMIT 5
);

-- With a shortlist covering all rows, the codes path must reproduce the exact brute-force top-k
-- that the deferred row policy admits.
WITH (SELECT vec FROM tab_vec_quantized_row_policy WHERE id = 43) AS ref
SELECT 'deferred_row_policy_exact',
    (SELECT groupArray(id) FROM
        (SELECT id, L2Distance(vec, ref) AS d FROM tab_vec_quantized_row_policy FINAL ORDER BY d, id LIMIT 5))
    = (SELECT groupArray(id) FROM
        (SELECT id, L2Distance(vec, ref) AS d FROM tab_vec_quantized_row_policy FINAL ORDER BY d, id LIMIT 5
         SETTINGS vector_search_use_quantized_codes = 0));

-- The rewrite must not let the policy through: every returned row satisfies it.
WITH (SELECT vec FROM tab_vec_quantized_row_policy WHERE id = 43) AS ref
SELECT 'deferred_row_policy_applied',
    min(tenant) = 1
FROM
(
    SELECT tenant FROM tab_vec_quantized_row_policy FINAL
    ORDER BY L2Distance(vec, ref) ASC
    LIMIT 5
);

DROP ROW POLICY 05153_vector_row_policy ON tab_vec_quantized_row_policy;
DROP TABLE tab_vec_quantized_row_policy;
