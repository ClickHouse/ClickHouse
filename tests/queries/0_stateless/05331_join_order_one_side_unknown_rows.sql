-- Join order of TPC-H Q5 when only some relations have row estimates and none has NDV statistics,
-- as for Iceberg tables whose build sides got sizes from the hash table statistics cache.
-- A join with a relation of unknown size is costed as before the FK->PK heuristic, so the key of
-- `orders` (a relation with a known size) keeps making the join of `lineitem` with the `customer` x `orders`
-- sub-join cheap, and that sub-join is built first.
-- https://github.com/ClickHouse/ClickHouse/issues/120921

SET allow_experimental_analyzer = 1;
SET explain_query_plan_default = 'legacy';
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_algorithm = 'greedy';
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 'auto';
SET use_hash_table_stats_for_join_reordering = 0;
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;
SET use_statistics = 0;
SET join_use_nulls = 1;
-- Pin the settings randomized in CI that change the join graph or the plan shape (to their defaults).
SET automatic_parallel_replicas_mode = 0;
SET enable_join_transitive_predicates = 1;
SET use_join_disjunctions_push_down = 1;
SET query_plan_join_shard_by_pk_ranges = 0;
SET query_plan_convert_outer_join_to_inner_join = 1;
SET query_plan_merge_filter_into_join_condition = 1;
SET query_plan_merge_filters = 1;
SET query_plan_remove_unused_columns = 1;
SET max_rows_in_join = 0;
SET max_bytes_in_join = 0;

CREATE TABLE region (r_regionkey Int32, r_name String) ENGINE = MergeTree ORDER BY r_regionkey SETTINGS auto_statistics_types = '';
CREATE TABLE nation (n_nationkey Int32, n_name String, n_regionkey Int32) ENGINE = MergeTree ORDER BY n_nationkey SETTINGS auto_statistics_types = '';
CREATE TABLE supplier (s_suppkey Int32, s_nationkey Int32) ENGINE = MergeTree ORDER BY s_suppkey SETTINGS auto_statistics_types = '';
CREATE TABLE customer (c_custkey Int32, c_nationkey Int32) ENGINE = MergeTree ORDER BY c_custkey SETTINGS auto_statistics_types = '';
CREATE TABLE orders (o_orderkey Int32, o_custkey Int32, o_orderdate Date) ENGINE = MergeTree ORDER BY o_orderkey SETTINGS auto_statistics_types = '';
CREATE TABLE lineitem (l_orderkey Int32, l_suppkey Int32, l_extendedprice Decimal(15, 2), l_discount Decimal(15, 2)) ENGINE = MergeTree ORDER BY l_orderkey SETTINGS auto_statistics_types = '';

-- One row per table, so the reads are not optimized away; the row counts come from the hints.
INSERT INTO region VALUES (2, 'ASIA');
INSERT INTO nation VALUES (1, 'N', 2);
INSERT INTO supplier VALUES (1, 1);
INSERT INTO customer VALUES (1, 1);
INSERT INTO orders VALUES (1, 1, '1994-06-01');
INSERT INTO lineitem VALUES (1, 1, 1, 0);

-- A hint without `cardinality` leaves the row count of the relation unknown.
SET param__internal_join_table_stat_hints = '{
    "customer": {},
    "orders":   { "cardinality": 2278186 },
    "lineitem": {},
    "supplier": {},
    "nation":   {},
    "region":   { "cardinality": 1 }
}';

SELECT replaceOne(explain, currentDatabase() || '.', '') FROM
(
    EXPLAIN
    SELECT n_name, sum(l_extendedprice * (1 - l_discount)) AS revenue
    FROM customer, orders, lineitem, supplier, nation, region
    WHERE c_custkey = o_custkey
        AND l_orderkey = o_orderkey
        AND l_suppkey = s_suppkey
        AND c_nationkey = s_nationkey
        AND s_nationkey = n_nationkey
        AND n_regionkey = r_regionkey
        AND r_name = 'ASIA'
        AND o_orderdate >= DATE '1994-01-01'
        AND o_orderdate < DATE '1995-01-01'
    GROUP BY n_name
    ORDER BY revenue DESC
)
WHERE explain LIKE '%Join (%' OR explain LIKE '%ReadFromMergeTree%';

DROP TABLE region;
DROP TABLE nation;
DROP TABLE supplier;
DROP TABLE customer;
DROP TABLE orders;
DROP TABLE lineitem;
