-- Tags: no-fasttest, no-ordinary-database, no-parallel-replicas
-- Regression: `vector_search_with_rescoring = 1` must not fall into the fused
-- range-reader path for vector columns whose element type is not `Float32`.
-- The fused path currently hardcodes `ColumnFloat32`; non-Float32 columns must
-- keep using the ExpressionStep-based rescore.

SET enable_analyzer = 1;
SET parallel_replicas_local_plan = 1;

DROP TABLE IF EXISTS tab_f64;
CREATE TABLE tab_f64(id Int32, vec Array(Float64), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2;
INSERT INTO tab_f64 VALUES
    (0, [1.0, 0.0]),(1, [1.1, 0.0]),(2, [1.2, 0.0]),(3, [1.3, 0.0]),(4, [1.4, 0.0]),
    (5, [0.0, 2.0]),(6, [0.0, 2.1]),(7, [0.0, 2.2]),(8, [0.0, 2.3]),(9, [0.0, 2.4]);

SELECT 'Array(Float64) with rescoring = 1';
WITH [0.0, 2.0] AS ref
SELECT id
FROM tab_f64
ORDER BY L2Distance(vec, ref)
LIMIT 3
SETTINGS vector_search_with_rescoring = 1;

DROP TABLE tab_f64;

DROP TABLE IF EXISTS tab_bf16;
CREATE TABLE tab_bf16(id Int32, vec Array(BFloat16), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2;
INSERT INTO tab_bf16 VALUES
    (0, [1.0, 0.0]),(1, [1.1, 0.0]),(2, [1.2, 0.0]),(3, [1.3, 0.0]),(4, [1.4, 0.0]),
    (5, [0.0, 2.0]),(6, [0.0, 2.1]),(7, [0.0, 2.2]),(8, [0.0, 2.3]),(9, [0.0, 2.4]);

SELECT 'Array(BFloat16) with rescoring = 1';
WITH [0.0, 2.0] AS ref
SELECT id
FROM tab_bf16
ORDER BY L2Distance(vec, ref)
LIMIT 3
SETTINGS vector_search_with_rescoring = 1;

DROP TABLE tab_bf16;
