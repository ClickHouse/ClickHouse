-- Tags: no-parallel-replicas
-- (the two-stage codes rewrite is deliberately disabled under parallel replicas, so the plan-shape assertions below
--  cannot hold there.)
-- Edge cases of the two-stage `Quantized(...)` planner rewrite: it must not corrupt or fail queries that select the
-- companion subcolumn, or that already contain a column named like the rewrite's internal sort key. In those cases the
-- rewrite either preserves the needed column or bails out to the exact path (the exact full-precision result is always
-- correct, just unaccelerated).

SET enable_quantized_codec = 1;
SET vector_search_use_quantized_codes = 1;
SET enable_analyzer = 1;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 1000000;

DROP TABLE IF EXISTS quantize_edge;
CREATE TABLE quantize_edge
(
    id UInt32,
    vec Array(Float32) CODEC(Quantized('rabitq', 64))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO quantize_edge (id, vec)
SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(64))
FROM numbers(2000);

-- Selecting the companion subcolumn alongside the distance ORDER BY must still work: the rewrite keeps `vec.quantized`
-- in the shortlist output (it is consumed by the rescore expression) instead of dropping it, so the query returns the
-- subcolumn for the top-k rather than failing with a missing column. rabitq codes are 64/8 + 4 = 12 bytes.
WITH (SELECT vec FROM quantize_edge WHERE id = 123) AS ref
SELECT 'selects_subcolumn', count(), countDistinct(length(q)) FROM
(
    SELECT id, vec.quantized AS q FROM quantize_edge
    ORDER BY cosineDistance(vec, ref) ASC LIMIT 5 SETTINGS vector_search_index_fetch_multiplier = 50
);

-- Positive oracle for the arm below, whose row-count assertions the unrewritten exact plan also satisfies.
-- `vector_search_index_fetch_multiplier` is range-checked inside the rewrite, after it has been admitted, so this error
-- is raised only when the rewrite really runs in the plan fragment. The initiator declines the rewrite while it is
-- building a distributed plan, so its EXPLAIN cannot show the shortlist steps and the plan shape cannot be asserted
-- directly. `max_rows_to_group_by` is pinned because the CI test profile sets it and `make_distributed_plan` rejects
-- aggregation with a non-zero value.
WITH (SELECT vec FROM quantize_edge WHERE id = 123) AS ref
SELECT count(), countDistinct(length(q)) FROM
(
    SELECT id, vec.quantized AS q FROM quantize_edge
    ORDER BY cosineDistance(vec, ref) ASC LIMIT 5
)
SETTINGS vector_search_index_fetch_multiplier = 0,
    make_distributed_plan = 1,
    distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0,
    enable_parallel_replicas = 0,
    max_rows_to_group_by = 0; -- { serverError INVALID_SETTING_VALUE }

-- Under a distributed plan the shortlist's internal sort key must not reach the rescore expression: nothing above
-- consumes it, so it used to be carried through as an extra trailing block column while the exchange steps above the
-- splice kept the width they were created with, aborting with "Invalid number of columns in chunk pushed to OutputPort.
-- Expected 2, found 3". The shortlist output needs two surviving columns for the widened block to reach an exchange,
-- which is why the subcolumn is selected here and `count()` alone would not cover the bug. The fetch multiplier goes in
-- the outer SETTINGS: a plan fragment is built from the top-level query context, so a subquery-level value never
-- reaches the shortlist.
WITH (SELECT vec FROM quantize_edge WHERE id = 123) AS ref
SELECT 'distributed_plan_subcolumn', count(), countDistinct(length(q)) FROM
(
    SELECT id, vec.quantized AS q FROM quantize_edge
    ORDER BY cosineDistance(vec, ref) ASC LIMIT 5
)
SETTINGS vector_search_index_fetch_multiplier = 50,
    make_distributed_plan = 1,
    distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0,
    enable_parallel_replicas = 0,
    max_rows_to_group_by = 0; -- pinned: the CI test profile sets it, and make_distributed_plan rejects aggregation with a non-zero value

-- A column named like the internal sort key forces the rewrite to bail to the exact path (no shortlist), and the query
-- still returns the correct exact top-k.
DROP TABLE IF EXISTS quantize_collide;
CREATE TABLE quantize_collide
(
    id UInt32,
    vec Array(Float32) CODEC(Quantized('rabitq', 64)),
    `__quantize_approx_distance` Float32
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO quantize_collide (id, vec, `__quantize_approx_distance`)
SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(64)), 0
FROM numbers(2000);

SELECT 'collision_bails',
    countIf(explain ILIKE '%quantized shortlist%') = 0 AS no_shortlist
FROM
(
    EXPLAIN PLAN
    SELECT id, `__quantize_approx_distance` FROM quantize_collide
    ORDER BY cosineDistance(vec, (SELECT vec FROM quantize_collide WHERE id = 123)) ASC LIMIT 5 SETTINGS vector_search_index_fetch_multiplier = 50
);

-- Even though it bails, the result is the exact brute-force top-k.
WITH (SELECT vec FROM quantize_collide WHERE id = 123) AS ref
SELECT 'collision_exact',
    (SELECT groupArray(id) FROM (SELECT id FROM quantize_collide ORDER BY cosineDistance(vec, ref) ASC, id LIMIT 10 SETTINGS vector_search_index_fetch_multiplier = 50))
    = (SELECT groupArray(id) FROM (SELECT id, cosineDistance(vec, ref) AS d FROM quantize_collide ORDER BY d, id LIMIT 10));

DROP TABLE quantize_collide;
DROP TABLE quantize_edge;
