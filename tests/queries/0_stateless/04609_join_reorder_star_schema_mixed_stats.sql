-- Regression test for the mixed-statistics case of the star-schema swap guard.
-- `RelationStats::column_stats` is populated per column, so a composite subtree
-- `(fact JOIN dim_a)` where only `dim_a` carries column statistics has a
-- non-empty stats map, while the fact table's next join key (`fact.key_b`)
-- still falls back to `estimated_rows` as its NDV proxy in `getColumnStats`,
-- underestimating the composite's cardinality exactly as in the stats-less
-- case. The guard must therefore key off the actual join-key columns of the
-- step rather than whole-map emptiness: an unrelated stat contributed by one
-- dimension must not disable the guard, or the large fact subtree is still
-- swapped to the hash-join build side.

SET query_plan_optimize_join_order_randomize = 0;
SET enable_analyzer = 1;
SET use_hash_table_stats_for_join_reordering = 0;
SET materialize_statistics_on_insert = 1;
SET use_statistics = 1;

DROP TABLE IF EXISTS fact;
DROP TABLE IF EXISTS dim_a;
DROP TABLE IF EXISTS dim_b;

-- Fact table: 100000 rows, join keys have low NDV, NO column statistics
-- (`auto_statistics_types` defaults to 'minmax, uniq', so it must be unset
-- explicitly to model a table without statistics).
CREATE TABLE fact (id UInt64, key_a UInt32, key_b UInt32, value UInt64)
    ENGINE = MergeTree ORDER BY id
    SETTINGS auto_statistics_types = '';
INSERT INTO fact SELECT number, number % 50, number % 100, number FROM numbers(100000);

-- dim_a is the only table with column statistics: its `key` stat is merged
-- into the composite `(fact JOIN dim_a)`, making its stats map non-empty.
CREATE TABLE dim_a (key UInt32, name String) ENGINE = MergeTree ORDER BY key
    SETTINGS auto_statistics_types = 'uniq';
INSERT INTO dim_a SELECT number, 'a_' || toString(number) FROM numbers(50);

-- dim_b has no statistics, so the second join estimates NDV of `fact.key_b`
-- with the `estimated_rows` fallback.
CREATE TABLE dim_b (key UInt32, name String) ENGINE = MergeTree ORDER BY key
    SETTINGS auto_statistics_types = '';
INSERT INTO dim_b SELECT number, 'b_' || toString(number) FROM numbers(100);

-- The fact table must appear as the first ReadFromMergeTree (outermost probe
-- side). Without the per-key check, the unrelated `dim_a.key` stat disables
-- the guard and dim_b appears first because the swap flips the outer join.
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
