-- Regression test for the transitive-predicate case of the star-schema swap guard.
-- With `enable_join_transitive_predicates`, join selectivity also comes from the
-- column-equivalence class spanning the two sides, and `cleanupJoinPredicates` may
-- drop or rewrite the step's own predicates afterwards: here `fact.key`, `dim_a.key`
-- and `dim_b.key` form one equivalence class, statistics exist only on the dimension
-- keys, and the predicate retained at the outer join step can end up being the
-- dimension-only one. The stats-less `fact.key` still dominates the class-wide max
-- NDV that costed the composite `(fact JOIN dim_a)`, so its estimate is proxy-based
-- and the guard must keep the fact subtree on the probe side. A guard that derives
-- "has statistics" from the final (post-cleanup) join predicates alone misses this.

SET query_plan_optimize_join_order_randomize = 0;
SET enable_analyzer = 1;
SET use_hash_table_stats_for_join_reordering = 0;
SET materialize_statistics_on_insert = 1;
SET use_statistics = 1;
SET enable_join_transitive_predicates = 1;

DROP TABLE IF EXISTS fact;
DROP TABLE IF EXISTS dim_a;
DROP TABLE IF EXISTS dim_b;

-- Fact table: 100000 rows, no column statistics at all, so every NDV lookup on
-- `fact.key` falls back to the `estimated_rows` proxy (`auto_statistics_types`
-- defaults to 'minmax, uniq', so it must be unset explicitly).
CREATE TABLE fact (id UInt64, key UInt32, value UInt64)
    ENGINE = MergeTree ORDER BY id
    SETTINGS auto_statistics_types = '';
INSERT INTO fact SELECT number, number % 50, number FROM numbers(100000);

CREATE TABLE dim_a (key UInt32 STATISTICS(uniq), name String) ENGINE = MergeTree ORDER BY key
    SETTINGS auto_statistics_types = '';
INSERT INTO dim_a SELECT number, 'a_' || toString(number) FROM numbers(50);

CREATE TABLE dim_b (key UInt32 STATISTICS(uniq), name String) ENGINE = MergeTree ORDER BY key
    SETTINGS auto_statistics_types = '';
INSERT INTO dim_b SELECT number, 'b_' || toString(number) FROM numbers(100);

-- The second join's predicate is written against `dim_a.key`, which has
-- statistics; only the equivalence class connects it to the stats-less
-- `fact.key`. The fact table must still appear as the first ReadFromMergeTree
-- (outermost probe side).
SELECT extract(explain, '(fact|dim_a|dim_b)')
FROM (
    EXPLAIN PLAN
    SELECT fact.id, dim_a.name, dim_b.name
    FROM fact
    INNER JOIN dim_a ON dim_a.key = fact.key
    INNER JOIN dim_b ON dim_b.key = dim_a.key
    SETTINGS query_plan_optimize_join_order_limit = 10
)
WHERE explain LIKE '%ReadFromMergeTree%';

DROP TABLE fact;
DROP TABLE dim_b;
DROP TABLE dim_a;
