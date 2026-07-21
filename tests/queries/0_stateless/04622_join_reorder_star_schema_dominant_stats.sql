-- Regression test for the precision of the star-schema swap guard's stats flag.
-- `computeSelectivity` estimates an equality as `1 / max(lhs_ndv, rhs_ndv)`, so a
-- missing statistic on one side only matters when its `estimated_rows` fallback
-- actually wins that max. Here `fact.key_a` has a real `uniq` statistic (~100000)
-- that dominates the proxy of the stats-less `dim_a` (50 rows), so the composite
-- `(fact JOIN dim_a)` estimate (~50 rows) is genuinely statistics-based and
-- accurate. The guard must NOT fire: treating "some lookup fell back to the
-- proxy" as poison would suppress the beneficial swap that builds the hash table
-- from the tiny composite instead of the larger `dim_b`.

SET query_plan_optimize_join_order_randomize = 0;
SET enable_analyzer = 1;
SET use_hash_table_stats_for_join_reordering = 0;
SET materialize_statistics_on_insert = 1;
SET use_statistics = 1;

DROP TABLE IF EXISTS fact;
DROP TABLE IF EXISTS dim_a;
DROP TABLE IF EXISTS dim_b;

-- Fact table: 100000 rows, a real `uniq` statistic only on the unique `key_a`
-- (the key of the first join). `auto_statistics_types` defaults to
-- 'minmax, uniq', so it must be unset explicitly to keep the other columns
-- without statistics.
CREATE TABLE fact (id UInt64, key_a UInt32 STATISTICS(uniq), key_b UInt32, value UInt64)
    ENGINE = MergeTree ORDER BY id
    SETTINGS auto_statistics_types = '';
INSERT INTO fact SELECT number, number, number % 100, number FROM numbers(100000);

CREATE TABLE dim_a (key UInt32, name String) ENGINE = MergeTree ORDER BY key
    SETTINGS auto_statistics_types = '';
INSERT INTO dim_a SELECT number, 'a_' || toString(number) FROM numbers(50);

CREATE TABLE dim_b (key UInt32, name String) ENGINE = MergeTree ORDER BY key
    SETTINGS auto_statistics_types = '';
INSERT INTO dim_b SELECT number, 'b_' || toString(number) FROM numbers(100);

-- The composite `(fact JOIN dim_a)` is correctly estimated at ~50 rows from the
-- real NDV of `fact.key_a` (the `dim_a` proxy of 50 loses the max), which is
-- smaller than `dim_b` (100 rows), so the swap moves the composite to the
-- hash-join build side and `dim_b` must appear as the first ReadFromMergeTree
-- (probe side). An imprecise flag that poisons the subtree merely because
-- `dim_a.key` lacks statistics would keep `fact` first.
SELECT extract(explain, '(fact|dim_a|dim_b)')
FROM (
    EXPLAIN PLAN
    SELECT fact.id, dim_a.name, dim_b.name
    FROM fact
    INNER JOIN dim_a ON dim_a.key = fact.key_a
    INNER JOIN dim_b ON dim_b.key = fact.key_b
    SETTINGS query_plan_optimize_join_order_limit = 10
)
WHERE explain LIKE '%ReadFromMergeTree%';

DROP TABLE fact;
DROP TABLE dim_b;
DROP TABLE dim_a;
