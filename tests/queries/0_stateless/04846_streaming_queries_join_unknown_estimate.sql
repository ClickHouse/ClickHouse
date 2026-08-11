-- Tags: no-parallel-replicas, no-darwin, no-random-settings, no-random-merge-tree-settings
-- `STREAM` index analysis is deferred to execution. Its empty `AnalysisResult` is
-- therefore unknown cardinality, not an exact zero for join-order estimation.

SET enable_streaming_queries = 1;
SET enable_analyzer = 1;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 0;
SET use_statistics = 0;

DROP TABLE IF EXISTS fact_04846;
DROP TABLE IF EXISTS dim_04846;

CREATE TABLE fact_04846 (id UInt64)
ENGINE = MergeTree ORDER BY id;

CREATE TABLE dim_04846 (id UInt64)
ENGINE = MergeTree ORDER BY id;

INSERT INTO fact_04846 SELECT number FROM numbers(10);
INSERT INTO dim_04846 SELECT number FROM numbers(5);

-- The `STREAM` side must be shown as unknown, not `f[0]`.
SELECT countIf(explain LIKE '%f[no_stats~?]%') = 1
FROM
(
    EXPLAIN
    SELECT count()
    FROM fact_04846 AS f STREAM
    INNER JOIN dim_04846 AS d ON f.id = d.id
)
WHERE explain LIKE '%⋈%';

DROP TABLE fact_04846;
DROP TABLE dim_04846;
