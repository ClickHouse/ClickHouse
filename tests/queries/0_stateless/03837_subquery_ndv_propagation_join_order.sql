SET allow_experimental_statistics = 1;

DROP TABLE IF EXISTS test_sales;
DROP TABLE IF EXISTS test_partners;
DROP TABLE IF EXISTS test_region_subsidies;
DROP TABLE IF EXISTS test_product_catalog;

CREATE TABLE test_sales (
    sale_id UInt64,
    product_id UInt64 STATISTICS(uniq),
    region_id UInt32 STATISTICS(uniq),
    amount Float64
) ENGINE = MergeTree ORDER BY sale_id;

CREATE TABLE test_partners (
    product_id UInt64 STATISTICS(uniq),
    partner_name String
) ENGINE = MergeTree ORDER BY product_id;

CREATE TABLE test_region_subsidies (
    region_id UInt32 STATISTICS(uniq),
    subsidy_type String,
    subsidy_amount Float64
) ENGINE = MergeTree ORDER BY region_id;

CREATE TABLE test_product_catalog (
    product_id UInt64 STATISTICS(uniq),
    catalog_info String
) ENGINE = MergeTree ORDER BY product_id;

-- sales: 50K rows, region_id NDV=5, product_id NDV=10K
INSERT INTO test_sales
    SELECT number, (number % 10000) + 1, (number % 5) + 1, number * 1.5
    FROM numbers(50000);

-- partners: 10K rows, product_id NDV=10K
INSERT INTO test_partners
    SELECT number + 1, concat('partner_', toString(number))
    FROM numbers(10000);

-- region_subsidies: 200 rows, region_id NDV=5 (40 per region)
INSERT INTO test_region_subsidies
    SELECT (number % 5) + 1, concat('subsidy_', toString(number)), number * 100.0
    FROM numbers(200);

-- product_catalog: 300 rows, product_id NDV=300
INSERT INTO test_product_catalog
    SELECT number + 1, concat('catalog_', toString(number))
    FROM numbers(300);

-- Merge parts to materialize column statistics
OPTIMIZE TABLE test_sales FINAL;
OPTIMIZE TABLE test_partners FINAL;
OPTIMIZE TABLE test_region_subsidies FINAL;
OPTIMIZE TABLE test_product_catalog FINAL;

SET enable_analyzer = 1;
SET allow_statistic_optimize = 1;
SET enable_parallel_replicas = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_algorithm = 'dpsize,greedy';
SET query_plan_join_swap_table = 0;
SET enable_join_runtime_filters = 0;


-- This query join order depends on whether statts for product_id (NDV=10K) and region_id (NDV=5) are properly propagated from subquery
SELECT explain FROM
(
EXPLAIN keep_logical_steps=1, actions=1
SELECT sq.region_id, rs.subsidy_type, sq.product_id, sq.total_amount, pc.catalog_info
FROM (
    SELECT s.region_id, s.product_id, sum(s.amount) AS total_amount
    FROM test_sales s
    JOIN test_partners p ON s.product_id = p.product_id
    GROUP BY s.region_id, s.product_id
) sq
JOIN test_region_subsidies rs ON sq.region_id = rs.region_id
JOIN test_product_catalog pc ON sq.product_id = pc.product_id
)
WHERE explain LIKE '% Join%' OR explain LIKE '% ResultRows:%' OR explain LIKE '% ReadFromMergeTree%' OR explain LIKE '% Aggregating%';

DROP TABLE test_sales;
DROP TABLE test_partners;
DROP TABLE test_region_subsidies;
DROP TABLE test_product_catalog;
