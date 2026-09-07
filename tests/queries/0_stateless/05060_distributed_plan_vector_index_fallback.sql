-- Tags: no-fasttest, no-old-analyzer
-- no-fasttest: the vector similarity index needs the usearch library, which is not in the fast test build.
-- no-old-analyzer: make_distributed_plan requires the analyzer.
-- Tracked in https://github.com/ClickHouse/ClickHouse/issues/118534

-- A vector similarity index with `ORDER BY distance LIMIT` under `make_distributed_plan`.
-- The index-only-scan rewrite (`optimizeVectorSearchWithVectorIndexSecondPass`) used to run on the
-- coordinator after the exchanges and the local top-N `Limit` had been inserted. It rebuilt the
-- `Expression` below the `Sorting` with the columns in a different order, so the `Sorting` output
-- header changed while the `Limit` above it kept the old header, and the fragment rebuild in
-- `makeDistributedPlan` threw `LOGICAL_ERROR` ("Cannot add step Limit to QueryPlan because it has
-- incompatible header with root step Sorting"). The pre-check that decides the fallback to local
-- execution did not catch it either.

DROP TABLE IF EXISTS t_vec_dist;

CREATE TABLE t_vec_dist (id UInt32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 256;

INSERT INTO t_vec_dist SELECT number, [toFloat32(number), toFloat32(number)] FROM numbers(1000);

SET make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0;
-- Pin everything that decides whether the index-only-scan rewrite is applied.
SET query_plan_try_use_vector_search = 1, use_skip_indexes = 1, vector_search_with_rescoring = 0, vector_search_filter_strategy = 'auto';
SET enable_cascades_optimizer = 0, allow_experimental_parallel_reading_from_replicas = 0;

-- The nearest neighbours of [1.2, 1.2] have distinct distances: id 1 (0.283), 2 (1.131), 0 (1.697), 3 (2.546).

SELECT 'plain';
SELECT id FROM t_vec_dist ORDER BY L2Distance(vec, [1.2, 1.2]) LIMIT 3;

SELECT 'distance in select';
SELECT id, round(L2Distance(vec, [1.2, 1.2]), 3) AS d FROM t_vec_dist ORDER BY d LIMIT 3;

SELECT 'where';
-- The post-filter keeps all three candidates returned by the index.
SELECT id FROM t_vec_dist WHERE id < 100 ORDER BY L2Distance(vec, [1.2, 1.2]) LIMIT 3;

SELECT 'offset';
SELECT id FROM t_vec_dist ORDER BY L2Distance(vec, [1.2, 1.2]) LIMIT 3 OFFSET 1;

SELECT 'subquery';
SELECT count() FROM (SELECT id FROM t_vec_dist ORDER BY L2Distance(vec, [1.2, 1.2]) LIMIT 3);

SELECT 'rescoring';
SELECT id FROM t_vec_dist ORDER BY L2Distance(vec, [1.2, 1.2]) LIMIT 3 SETTINGS vector_search_with_rescoring = 1;

-- Strict mode: the query must really be distributed (no fallback to local execution) and the
-- worker fragment must return the same result.
SELECT 'strict';
SELECT id FROM t_vec_dist ORDER BY L2Distance(vec, [1.2, 1.2]) LIMIT 3 SETTINGS distributed_plan_fallback_to_local_execution = 0;

SELECT 'strict rescoring';
SELECT id FROM t_vec_dist ORDER BY L2Distance(vec, [1.2, 1.2]) LIMIT 3 SETTINGS distributed_plan_fallback_to_local_execution = 0, vector_search_with_rescoring = 1;

SELECT 'plan has gather exchange';
SELECT countIf(explain LIKE '%GatherExchange%') > 0 FROM (EXPLAIN PLAN SELECT id FROM t_vec_dist ORDER BY L2Distance(vec, [1.2, 1.2]) LIMIT 3);

DROP TABLE t_vec_dist;
