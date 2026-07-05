-- Regression test: join reorder must not place a large fact table on the build
-- side when joining with small dimension tables. Without per-column NDV stats
-- the selectivity formula `1/max(NDV_l, NDV_r)` can underestimate the join
-- cardinality when `estimated_rows` is used as NDV proxy, causing the optimizer
-- to think the intermediate result is tiny and swap the wrong way.
--
-- The fix: automatic join side swapping should not move a composite subtree
-- containing a much larger base relation to the build side only because the
-- intermediate join cardinality was underestimated.
--
-- Without the fix, the intermediate result of `(fact JOIN dim_a)` is estimated
-- as ~50 rows, so the swap logic sees `50 < 100` (`dim_b`) and flips the join,
-- placing the fact-side on the build (right). With the fix, swap is not
-- triggered and the fact table stays on the probe (left) side.

SET query_plan_optimize_join_order_randomize = 0;
SET enable_analyzer = 1;
SET use_hash_table_stats_for_join_reordering = 0;

DROP TABLE IF EXISTS fact;
DROP TABLE IF EXISTS dim_a;
DROP TABLE IF EXISTS dim_b;

-- Fact table: 100000 rows, join keys have low NDV.
CREATE TABLE fact (id UInt64, key_a UInt32, key_b UInt32, value UInt64)
    ENGINE = MergeTree ORDER BY id;
INSERT INTO fact SELECT number, number % 50, number % 100, number FROM numbers(100000);

-- Dimension tables: small, primary key is the join key.
-- dim_a is smaller than dim_b to break cost ties deterministically.
CREATE TABLE dim_a (key UInt32, name String) ENGINE = MergeTree ORDER BY key;
INSERT INTO dim_a SELECT number, 'a_' || toString(number) FROM numbers(50);

CREATE TABLE dim_b (key UInt32, name String) ENGINE = MergeTree ORDER BY key;
INSERT INTO dim_b SELECT number, 'b_' || toString(number) FROM numbers(100);

-- Star schema join: fact table must appear as the first ReadFromMergeTree
-- (outermost probe side). Without the fix, dim_b appears first because the
-- swap logic incorrectly flips the outer join.
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
DROP TABLE dim_a;
DROP TABLE dim_b;
