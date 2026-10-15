-- Tags: no-fasttest, no-ordinary-database, no-parallel-replicas
-- no-parallel-replicas: the test asserts that the no-rescoring optimization applies, and with
-- parallel replicas the optimization is disabled (vector-search read hints are produced during
-- local index analysis).

-- The no-rescoring vector search rewrite is skipped when anything besides the ORDER BY distance
-- expression consumes the vector column. The check which resolves the vector column inside a filter
-- or row-policy DAG used to accept any input whose name merely ends in `.` plus the column name,
-- because the old analyzer can name the input with a table qualifier. A `Nested` column `n` with an
-- element `vec` is a physical column literally named `n.vec`, so a filter over `n.vec` was mistaken
-- for a consumer of `vec` and the optimization silently fell back to rescoring.

DROP TABLE IF EXISTS tab_vec_dotted;

CREATE TABLE tab_vec_dotted
(
    id Int32,
    vec Array(Float32),
    n Nested(vec Array(Float32)),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 100000000
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2;

INSERT INTO tab_vec_dotted VALUES (0, [1.0, 0.0], [[9.0]]), (1, [1.1, 0.0], [[9.0]]), (2, [1.2, 0.0], [[9.0]]), (3, [1.3, 0.0], [[9.0]]), (4, [1.4, 0.0], [[9.0]]), (5, [0.0, 2.0], [[9.0]]), (6, [0.0, 2.1], [[9.0]]), (7, [0.0, 2.2], [[9.0]]), (8, [0.0, 2.3], [[9.0]]), (9, [0.0, 2.4], [[9.0]]);

-- `query_plan_optimize_lazy_materialization = 0` pins the plan shape the rewrite matches
-- (lazy materialization would otherwise replace it and hide the rewrite).

SELECT 'a filter over the unrelated dotted column returns the exact top-k';
SELECT id FROM tab_vec_dotted
WHERE has(n.vec, [9.])
ORDER BY L2Distance(vec, [0., 2.]) ASC
LIMIT 3
SETTINGS vector_search_with_rescoring = 0, query_plan_optimize_lazy_materialization = 0;

-- `Sort description: sqrt(_distance)` is present only when the no-rescoring rewrite is applied.
-- `make_distributed_plan = 0` is required on the outer SELECT: the `distributed plan` job enables it
-- globally and `SELECT count() FROM (EXPLAIN ...)` then throws `SUPPORT_IS_DISABLED`.

SELECT 'the rewrite applies although the filter mentions the dotted column';
SELECT count() FROM
(
    EXPLAIN SELECT id FROM tab_vec_dotted
    WHERE has(n.vec, [9.])
    ORDER BY L2Distance(vec, [0., 2.]) ASC
    LIMIT 3
    SETTINGS vector_search_with_rescoring = 0, query_plan_optimize_lazy_materialization = 0
)
WHERE explain LIKE '%Sort description: sqrt(_distance)%'
SETTINGS make_distributed_plan = 0;

-- A genuine consumer of the vector column must still disable the rewrite.

SELECT 'a filter over the vector column itself still disables the rewrite';
SELECT count() FROM
(
    EXPLAIN SELECT id FROM tab_vec_dotted
    WHERE length(vec) = 2
    ORDER BY L2Distance(vec, [0., 2.]) ASC
    LIMIT 3
    SETTINGS vector_search_with_rescoring = 0, query_plan_optimize_lazy_materialization = 0
)
WHERE explain LIKE '%Sort description: sqrt(_distance)%'
SETTINGS make_distributed_plan = 0;

SELECT id FROM tab_vec_dotted
WHERE length(vec) = 2
ORDER BY L2Distance(vec, [0., 2.]) ASC
LIMIT 3
SETTINGS vector_search_with_rescoring = 0, query_plan_optimize_lazy_materialization = 0;

DROP TABLE tab_vec_dotted;
