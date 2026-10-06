-- Tags: no-fasttest, no-ordinary-database, no-parallel-replicas
-- no-parallel-replicas: the vector search plan rewrites are disabled with parallel replicas.

-- Vector search settings in the SETTINGS clause of a subquery or of a UNION ALL leg apply to that part of the query (#124087).
SET explain_query_plan_default = 'legacy';
SET parallel_replicas_local_plan = 1;

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2;
INSERT INTO tab VALUES (0, [1.1, 0.0]), (1, [1.3, 0.0]), (2, [0.0, 2.7]), (3, [0.0, 3.3]), (4, [5.3, 0.0]), (5, [0.0, 7.1]);

SELECT '-- A1 trailing clause of a single SELECT: exact distances';
SELECT id, L2Distance(vec, [1.0, 0.0]) AS d FROM tab ORDER BY d LIMIT 2
SETTINGS vector_search_with_rescoring = 1;

SELECT '-- A2 clause of a subquery in FROM: exact distances';
SELECT id, d FROM (SELECT id, L2Distance(vec, [1.0, 0.0]) AS d FROM tab ORDER BY d LIMIT 2
    SETTINGS vector_search_with_rescoring = 1);

SELECT '-- A3 clause of the first leg of a UNION ALL: exact distances';
SELECT id, L2Distance(vec, [1.0, 0.0]) AS d FROM tab ORDER BY d LIMIT 2
    SETTINGS vector_search_with_rescoring = 1
UNION ALL
SELECT toInt32(0), toFloat32(0) WHERE 0;

SELECT '-- A4 clause of both legs of a UNION ALL in FROM: exact distances';
SELECT leg, id, d FROM
(
    SELECT 1 AS leg, id, L2Distance(vec, [1.0, 0.0]) AS d FROM tab ORDER BY d LIMIT 2
        SETTINGS vector_search_with_rescoring = 1
    UNION ALL
    SELECT 2 AS leg, id, L2Distance(vec, [1.0, 0.0]) AS d FROM tab ORDER BY d LIMIT 2
        SETTINGS vector_search_with_rescoring = 1
)
ORDER BY leg, id;

SELECT '-- A5 trailing clause of a UNION ALL still covers the first leg: exact distances';
SELECT id, L2Distance(vec, [1.0, 0.0]) AS d FROM tab ORDER BY d LIMIT 2
UNION ALL
SELECT toInt32(0), toFloat32(0) WHERE 0
SETTINGS vector_search_with_rescoring = 1;

SELECT '-- B1 max_limit_for_vector_search_queries of a subquery: no index, exact distances';
SELECT id, d FROM (SELECT id, L2Distance(vec, [1.0, 0.0]) AS d FROM tab ORDER BY d LIMIT 3
    SETTINGS max_limit_for_vector_search_queries = 2);

SELECT '-- B2 same with rescoring: all 3 rows';
SELECT id, d FROM (SELECT id, L2Distance(vec, [1.0, 0.0]) AS d FROM tab ORDER BY d LIMIT 3
    SETTINGS max_limit_for_vector_search_queries = 2, vector_search_with_rescoring = 1);

SET vector_search_with_rescoring = 1;

SELECT '-- A6 session setting reaches a subquery: exact distances';
SELECT id, d FROM (SELECT id, L2Distance(vec, [1.0, 0.0]) AS d FROM tab ORDER BY d LIMIT 2);

SELECT '-- A7 clause of a subquery turns rescoring off: the index-only plan is used';
SELECT countIf(position(explain, '_distance') > 0) > 0 FROM (
    EXPLAIN header = 1
    SELECT id, d FROM (SELECT id, L2Distance(vec, [1.0, 0.0]) AS d FROM tab ORDER BY d LIMIT 2
        SETTINGS vector_search_with_rescoring = 0));

SET vector_search_with_rescoring = 0;

DROP TABLE tab;

-- Data of 02354_vector_search_pre_and_post_filtering: the 3 nearest neighbours in partition 2025-01-03 fail the filter.
DROP TABLE IF EXISTS tab_filter;
CREATE TABLE tab_filter
(
    id Int32,
    date Date,
    attr2 Int32,
    vec Array(Float32),
    INDEX idx_vec vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 10000
)
ENGINE = MergeTree PARTITION BY date ORDER BY id SETTINGS index_granularity = 3;
INSERT INTO tab_filter VALUES
  (1, '2025-01-01', 1001, [1.0, 0.0]), (2, '2025-01-01', 1002, [1.1, 0.0]), (3, '2025-01-01', 1003, [1.2, 0.0]),
  (4, '2025-01-02', 1003, [1.3, 0.0]), (5, '2025-01-02', 1004, [1.4, 0.0]), (6, '2025-01-02', 1005, [1.5, 0.0]),
  (7, '2025-01-03', 1005, [1.6, 0.0]), (8, '2025-01-03', 1006, [1.7, 0.0]), (9, '2025-01-03', 1007, [1.8, 0.0]),
  (10, '2025-01-03', 1008, [1.9, 0.0]), (11, '2025-01-03', 1009, [2.0, 0.0]), (12, '2025-01-03', 1010, [2.1, 0.0]);

SELECT '-- C1 prefilter as the trailing clause of a single SELECT';
SELECT id FROM tab_filter WHERE date = '2025-01-03' AND attr2 >= 1008 ORDER BY L2Distance(vec, [1.0, 1.0]) LIMIT 3
SETTINGS vector_search_filter_strategy = 'prefilter';

SELECT '-- C2 prefilter as the clause of a subquery in FROM';
SELECT id FROM (SELECT id FROM tab_filter WHERE date = '2025-01-03' AND attr2 >= 1008
    ORDER BY L2Distance(vec, [1.0, 1.0]) LIMIT 3 SETTINGS vector_search_filter_strategy = 'prefilter');

DROP TABLE tab_filter;

-- Quantized-codes rewrite (experimental codec): max_limit_for_vector_search_queries of a subquery must disable it.
SET enable_quantized_codec = 1;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 1000000;

DROP TABLE IF EXISTS tab_codes;
CREATE TABLE tab_codes (id UInt32, vec Array(Float32) CODEC(Quantized('int8', 64))) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab_codes SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(64))
FROM numbers(4000);

SELECT '-- D1 the subquery uses the quantized shortlist';
SELECT countIf(explain ILIKE '%quantized shortlist%') > 0 FROM (
    EXPLAIN PLAN
    SELECT id FROM (SELECT id FROM tab_codes ORDER BY L2Distance(vec, (SELECT vec FROM tab_codes WHERE id = 123)) LIMIT 3
        SETTINGS vector_search_use_quantized_codes = 1));

SELECT '-- D2 max_limit_for_vector_search_queries of the subquery disables it';
SELECT countIf(explain ILIKE '%quantized shortlist%') = 0 FROM (
    EXPLAIN PLAN
    SELECT id FROM (SELECT id FROM tab_codes ORDER BY L2Distance(vec, (SELECT vec FROM tab_codes WHERE id = 123)) LIMIT 3
        SETTINGS vector_search_use_quantized_codes = 1, max_limit_for_vector_search_queries = 2));

DROP TABLE tab_codes;
