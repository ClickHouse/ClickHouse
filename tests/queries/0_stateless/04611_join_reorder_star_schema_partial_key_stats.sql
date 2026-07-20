-- Regression test for the partial-statistics case of the star-schema swap guard.
-- `computeSelectivity` uses the `estimated_rows` NDV proxy for every join key
-- without column statistics, and the resulting underestimation of a composite
-- subtree propagates upward: here `(fact JOIN dim_a ON key_a)` is already
-- underestimated because `fact.key_a` has no statistics, so at the next join
-- the composite looks smaller than `dim_b` even though `fact.key_b` does have
-- statistics. A guard that only inspects the keys of the current step trusts
-- that estimate and still swaps the large fact subtree to the hash-join build
-- side; the guard must instead consider whether any step inside the left
-- subtree fell back to the proxy.

SET query_plan_optimize_join_order_randomize = 0;
SET enable_analyzer = 1;
SET use_hash_table_stats_for_join_reordering = 0;
SET materialize_statistics_on_insert = 1;
SET use_statistics = 1;

DROP TABLE IF EXISTS fact;
DROP TABLE IF EXISTS dim_a;
DROP TABLE IF EXISTS dim_b;

-- Fact table: 100000 rows, column statistics only for `key_b` — the key of the
-- SECOND join. The first join's key (`key_a`) has none, so the composite
-- `(fact JOIN dim_a)` is estimated with the `estimated_rows` NDV proxy
-- (`auto_statistics_types` defaults to 'minmax, uniq', so it must be unset
-- explicitly to keep the other columns without statistics).
CREATE TABLE fact (id UInt64, key_a UInt32, key_b UInt32 STATISTICS(uniq), value UInt64)
    ENGINE = MergeTree ORDER BY id
    SETTINGS auto_statistics_types = '';
INSERT INTO fact SELECT number, number % 50, number % 100, number FROM numbers(100000);

CREATE TABLE dim_a (key UInt32, name String) ENGINE = MergeTree ORDER BY key
    SETTINGS auto_statistics_types = '';
INSERT INTO dim_a SELECT number, 'a_' || toString(number) FROM numbers(50);

CREATE TABLE dim_b (key UInt32, name String) ENGINE = MergeTree ORDER BY key
    SETTINGS auto_statistics_types = '';
INSERT INTO dim_b SELECT number, 'b_' || toString(number) FROM numbers(100);

-- The fact table must appear as the first ReadFromMergeTree (outermost probe
-- side). If the guard only checks the current step's keys, the `fact.key_b`
-- statistic disables it and dim_b appears first because the swap moves the
-- underestimated fact subtree to the build side.
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
