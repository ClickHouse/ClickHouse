-- Tags: no-fasttest, no-ordinary-database, no-parallel-replicas
-- no-parallel-replicas: the test asserts that the no-rescoring optimization applies, and with
-- parallel replicas the optimization is disabled (vector-search read hints are produced during
-- local index analysis).

-- The no-rescoring vector search rewrite removes the vector column from the read header. It must be
-- skipped when anything besides the ORDER BY distance expression still consumes the vector column.
-- The rewrite used to check only whether the column itself (or an alias of it) is an output of the
-- expression under the `Sorting` step, so an indirect consumer in the filter, e.g. `length(vec) = 2`,
-- kept the column INPUT alive in the filter DAG and the query failed with `NOT_FOUND_COLUMN_IN_BLOCK`.

DROP TABLE IF EXISTS tab_vec_consumers;

CREATE TABLE tab_vec_consumers
(
    id Int32,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 100000000
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2;

INSERT INTO tab_vec_consumers VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]), (5, [0.0, 2.0]), (6, [0.0, 2.1]), (7, [0.0, 2.2]), (8, [0.0, 2.3]), (9, [0.0, 2.4]);

-- `query_plan_optimize_lazy_materialization = 0` pins the plan shape the rewrite matches
-- (lazy materialization would otherwise replace it and hide the bug).

SELECT 'filter consumes the vector column';
SELECT id FROM tab_vec_consumers
WHERE length(vec) = 2
ORDER BY L2Distance(vec, [0., 2.]) ASC
LIMIT 3
SETTINGS vector_search_with_rescoring = 0, query_plan_optimize_lazy_materialization = 0;

SELECT 'SELECT consumes the vector column indirectly';
SELECT length(vec), id FROM tab_vec_consumers
ORDER BY L2Distance(vec, [0., 2.]) ASC
LIMIT 3
SETTINGS vector_search_with_rescoring = 0, query_plan_optimize_lazy_materialization = 0;

SELECT 'SELECT contains another distance expression over the vector column';
SELECT id, round(L2Distance(vec, [0., 2.1]), 3) FROM tab_vec_consumers
ORDER BY L2Distance(vec, [0., 2.]) ASC
LIMIT 3
SETTINGS vector_search_with_rescoring = 0, query_plan_optimize_lazy_materialization = 0;

SELECT 'the filter consuming the vector column works with the old analyzer as well';
SELECT id FROM tab_vec_consumers
WHERE length(vec) = 2
ORDER BY L2Distance(vec, [0., 2.]) ASC
LIMIT 3
SETTINGS vector_search_with_rescoring = 0, query_plan_optimize_lazy_materialization = 0, enable_analyzer = 0;

SELECT 'the rewrite still applies when only the ORDER BY consumes the vector column';
SELECT count() FROM
(
    EXPLAIN SELECT id FROM tab_vec_consumers
    ORDER BY L2Distance(vec, [0., 2.]) ASC
    LIMIT 3
    SETTINGS vector_search_with_rescoring = 0, query_plan_optimize_lazy_materialization = 0
)
WHERE explain LIKE '%Sort description: sqrt(_distance)%';

DROP TABLE tab_vec_consumers;
