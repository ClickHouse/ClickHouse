-- Tags: no-random-settings

-- Regression test for normal projections with parallel replicas.
-- Verifies that the projection optimization is applied and produces
-- correct results when PREWHERE is involved (the WHERE filter on a
-- column not in SELECT gets moved to PREWHERE by optimizePrewhere).

SET enable_analyzer = 1;
SET allow_experimental_parallel_reading_from_replicas = 2;
SET max_parallel_replicas = 3;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_local_plan = 1;
SET optimize_aggregation_in_order = 0;
SET parallel_replicas_support_projection = 1;
SET query_plan_optimize_lazy_materialization = 0;

DROP TABLE IF EXISTS test_projection_pr;

CREATE TABLE test_projection_pr
(
    id UInt64,
    url String,
    region String,
    payload String,
    PROJECTION region_proj
    (
        SELECT url, region, payload ORDER BY region
    )
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 32;

-- Insert enough rows so that column sizes are meaningful and
-- optimizePrewhere definitely moves the WHERE filter to PREWHERE.
INSERT INTO test_projection_pr
    SELECT
        number,
        'https://example.com/page' || toString(number),
        if(number % 3 = 0, 'europe', if(number % 3 = 1, 'us_west', 'asia')),
        randomPrintableASCII(200)
    FROM numbers(10000);

-- Single part so the projection covers all data.
OPTIMIZE TABLE test_projection_pr FINAL;

-- The query selects only `url` and filters on `region`.
-- The projection is ordered by `region`, giving it fewer marks to read.
SELECT url FROM test_projection_pr WHERE region = 'europe' ORDER BY url LIMIT 1;

-- Verify that the projection optimization is actually applied: the plan must
-- use region_proj (the normal projection) for the local read path inside
-- the parallel replicas Union.
SELECT count() > 0
FROM (
    EXPLAIN PLAN
    SELECT url FROM test_projection_pr WHERE region = 'europe' ORDER BY url LIMIT 1
)
WHERE explain LIKE '%region_proj%';

DROP TABLE test_projection_pr;
