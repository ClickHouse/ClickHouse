-- Tags: no-flaky-check
-- no-flaky-check: every distributed-plan statement pays for a full optimizer run and multi-stage
-- execution, which is ~50x slower in debug builds; the flaky check's repeated runs exceed its budget.
-- Verifies the join order and distributed execution strategy of `Q18` (`rewrite_in_to_join`) with
-- `cascades_aggregation_pushdown` on, using SF100 cardinalities injected via
-- `_internal_join_table_stat_hints`; also pins a customer/nation query that genuinely gets pushed
-- under the same SF100 hints, as a regression canary for the rule's reliable-cardinality gate
-- (see `AggregationPushdown::buildPushdownAlternative`) actually firing when it honestly should.

-- Pushdown-enabled twin of `03836_tpch_join_order_plans`, trimmed to `Q18`
-- (`rewrite_in_to_join`): the only TPC-H query whose plan legitimately differs with
-- `cascades_aggregation_pushdown` on. The cardinality gate still rejects pushing the outer
-- aggregation's widened `GROUP BY` key set (`c_name`, `c_custkey`, `o_orderkey`, `o_orderdate`,
-- `o_totalprice`) into the orders/customer side - at SF100 it is close to cardinality-unique,
-- so the proven composite bound offers no guaranteed shrinkage. What fires instead is the
-- lineitem-side variant A: no `GROUP BY` key comes from `lineitem`, so the pushed key set is
-- just the join-condition column `l_orderkey` (NDV 150M vs 600M rows - a proven 4x shrinkage,
-- and 4x fewer rows through the shuffle below the top join). It wins because the merge-only
-- `Aggregating` above the join takes the Shuffle strategy directly on the join's `o_orderkey`
-- partitioning, with no extra exchange - a plan the previous `MergingAggregatedStep`-based merge
-- (no distribution strategies, full gather below it) could not express.
-- Every other TPC-H query is already asserted pushdown-off by `03836`; their pushdown-on shapes
-- turned out to sit on near-ties in the Cascades cost model that resolve differently per build
-- flavor/machine, so asserting them here produced environment-flaky failures rather than
-- catching real regressions.
SET cascades_aggregation_pushdown = 1;

DROP TABLE IF EXISTS region;
DROP TABLE IF EXISTS nation;
DROP TABLE IF EXISTS part;
DROP TABLE IF EXISTS supplier;
DROP TABLE IF EXISTS partsupp;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS lineitem;

-- SETTINGS pin `auto_statistics_types=''` and `min_bytes_for_wide_part` to
-- so real statistics do not interfere with the hints
CREATE TABLE region (
    r_regionkey Int32, r_name String, r_comment String
) ENGINE = MergeTree() ORDER BY r_regionkey
  SETTINGS auto_statistics_types = '', min_bytes_for_wide_part = 10737418240;

CREATE TABLE nation (
    n_nationkey Int32, n_name String, n_regionkey Int32, n_comment String
) ENGINE = MergeTree() ORDER BY n_nationkey
  SETTINGS auto_statistics_types = '', min_bytes_for_wide_part = 10737418240;

CREATE TABLE part (
    p_partkey Int32, p_name String, p_mfgr String, p_brand String,
    p_type String, p_size Int32, p_container String,
    p_retailprice Decimal(15,2), p_comment String
) ENGINE = MergeTree() ORDER BY p_partkey
  SETTINGS auto_statistics_types = '', min_bytes_for_wide_part = 10737418240;

CREATE TABLE supplier (
    s_suppkey Int32, s_name String, s_address String, s_nationkey Int32,
    s_phone String, s_acctbal Decimal(15,2), s_comment String
) ENGINE = MergeTree() ORDER BY s_suppkey
  SETTINGS auto_statistics_types = '', min_bytes_for_wide_part = 10737418240;

CREATE TABLE partsupp (
    ps_partkey Int32, ps_suppkey Int32, ps_availqty Int32,
    ps_supplycost Decimal(15,2), ps_comment String
) ENGINE = MergeTree() ORDER BY (ps_partkey, ps_suppkey)
  SETTINGS auto_statistics_types = '', min_bytes_for_wide_part = 10737418240;

CREATE TABLE customer (
    c_custkey Int32, c_name String, c_address String, c_nationkey Int32,
    c_phone String, c_acctbal Decimal(15,2), c_mktsegment String, c_comment String
) ENGINE = MergeTree() ORDER BY c_custkey
  SETTINGS auto_statistics_types = '', min_bytes_for_wide_part = 10737418240;

CREATE TABLE orders (
    o_orderkey Int32, o_custkey Int32, o_orderstatus String,
    o_totalprice Decimal(15,2), o_orderdate Date, o_orderpriority String,
    o_clerk String, o_shippriority Int32, o_comment String
) ENGINE = MergeTree() ORDER BY o_orderkey
  SETTINGS auto_statistics_types = '', min_bytes_for_wide_part = 10737418240;

CREATE TABLE lineitem (
    l_orderkey Int32, l_partkey Int32, l_suppkey Int32, l_linenumber Int32,
    l_quantity Decimal(15,2), l_extendedprice Decimal(15,2), l_discount Decimal(15,2),
    l_tax Decimal(15,2), l_returnflag String, l_linestatus String,
    l_shipdate Date, l_commitdate Date, l_receiptdate Date,
    l_shipinstruct String, l_shipmode String, l_comment String
) ENGINE = MergeTree() ORDER BY (l_orderkey, l_linenumber)
  SETTINGS auto_statistics_types = '', min_bytes_for_wide_part = 10737418240;

-- One sentinel row per table prevents 0-row short-circuit optimizations.
INSERT INTO region    VALUES (1, 'A', '');
INSERT INTO nation    VALUES (1, 'A', 1, '');
INSERT INTO part      VALUES (1, 'a', 'M', 'B', 'T', 1, 'C', 1.0, '');
INSERT INTO supplier  VALUES (1, 'A', 'A', 1, '0', 1.0, '');
INSERT INTO partsupp  VALUES (1, 1, 1, 1.0, '');
INSERT INTO customer  VALUES (1, 'A', 'A', 1, '0', 1.0, 'B', '');
INSERT INTO orders    VALUES (1, 1, 'O', 1.0, '1994-01-01', '1-URGENT', 'C1', 0, '');
INSERT INTO lineitem  VALUES (1, 1, 1, 1, 1.0, 1.0, 0.0, 0.0, 'N', 'O', '1994-02-01', '1994-01-15', '1994-02-05', 'DELIVER IN PERSON', 'SHIP', '');

SET explain_query_plan_default = 'legacy';
SET allow_experimental_analyzer = 1;
SET enable_join_runtime_filters = 0;
SET use_join_disjunctions_push_down = 1;
SET query_plan_optimize_join_order_limit = 10;
SET allow_statistic_optimize = 1;
SET query_plan_optimize_join_order_algorithm = 'dpsize,greedy';
SET make_distributed_plan = 1;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET distributed_plan_execute_locally = 1;
SET enable_cascades_optimizer = 1;
-- The test profile installed in CI sets a non-zero max_rows_to_group_by, which keeps
-- aggregations local.  Pin it to 0 so distributed two-phase aggregation is exercised.
SET max_rows_to_group_by = 0;
-- `IN (subquery)` runs as a set built on the initiator (the default): the set values ship with
-- the worker tasks and filter at the reads. `rewrite_in_to_join` stays off; the explicit
-- rewrite is covered by 04653 and by the per-query variants below.
SET rewrite_in_to_join = 0;
-- Without this, index analysis builds the `IN` sets during planning from the single synthetic
-- row per table; an empty set prunes the reads to zero ranges and collapses the plan to one
-- node, so the asserted shape would depend on the fake data instead of the hints.
SET use_index_for_in_with_subqueries = 0;
SET correlated_subqueries_use_in_memory_buffer = 0;
SET allow_experimental_correlated_subqueries = 1;
-- The CI test profile sets non-zero max_rows_in_join/max_bytes_in_join, which alters the
-- correlated-subquery join order. Pin to 0 so the asserted plan is stable.
SET max_rows_in_join = 0;
SET max_bytes_in_join = 0;
SET query_plan_join_swap_table = 0;
-- The Cascades cost model's parallelism input follows `max_threads`; pin it so `Q18`'s
-- plan does not depend on the machine's core count.
SET max_threads = 32;
-- Pin the plan-shaping optimizations (to their defaults) so randomized settings cannot
-- change the asserted plan. query_plan_optimize_join_order_randomize must stay off.
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_convert_outer_join_to_inner_join = 1;
SET query_plan_convert_any_join_to_semi_or_anti_join = 1;
SET query_plan_merge_filter_into_join_condition = 1;
SET query_plan_merge_filters = 1;
SET query_plan_remove_unused_columns = 1;
SET query_plan_optimize_prewhere = 1;
SET optimize_move_to_prewhere = 1;
SET optimize_extract_common_expressions = 1;
SET optimize_syntax_fuse_functions = 1;
SET optimize_and_compare_chain = 1;
SET enable_join_transitive_predicates = 1;
SET send_logs_level = 'error';

-- Simulate 20 node cluster, and set cost weights to optimize for lower sequential time, i.e. more parallelism
SET param__internal_cascades_cluster_node_count = 20;
SET param__internal_cascades_cost_config = '{
    "work_weight":1,
    "exchange_fixed_overhead":3000,
    "network_weight":1,
    "sequential_weight":32
}';

-- SF100 baseline cardinalities, bytes_per_row, and key column NDVs for all TPC-H tables.
-- bytes_per_row reflects typical uncompressed SF100 row widths.
-- Individual queries override this with post-filter cardinalities where needed.
SET param__internal_join_table_stat_hints = '{
    "lineitem": { "cardinality": 600037902, "avg_row_bytes": 128, "distinct_keys": { "l_orderkey": 150000000, "l_partkey": 20000000, "l_suppkey": 1000000, "l_linenumber": 7, "l_returnflag": 3, "l_linestatus": 2, "l_shipdate": 2526, "l_commitdate": 2466, "l_receiptdate": 2554, "l_quantity": 50, "l_discount": 11, "l_shipmode": 7, "l_shipinstruct": 4 } },
    "orders":   { "cardinality": 150000000, "avg_row_bytes": 80,  "distinct_keys": { "o_orderkey": 150000000, "o_custkey": 15000000, "o_orderdate": 2406, "o_orderstatus": 3, "o_orderpriority": 5, "o_clerk": 1000 } },
    "customer": { "cardinality": 15000000,  "avg_row_bytes": 120, "distinct_keys": { "c_custkey": 15000000, "c_nationkey": 25, "c_mktsegment": 5, "c_acctbal": 14975000, "c_phone": 14999997 } },
    "part":     { "cardinality": 20000000,  "avg_row_bytes": 90,  "distinct_keys": { "p_partkey": 20000000, "p_type": 150, "p_brand": 25, "p_size": 50, "p_container": 40, "p_name": 19999999 } },
    "supplier": { "cardinality": 1000000,   "avg_row_bytes": 110, "distinct_keys": { "s_suppkey": 1000000, "s_nationkey": 25, "s_acctbal": 999990 } },
    "partsupp": { "cardinality": 80000000,  "avg_row_bytes": 40,  "distinct_keys": { "ps_partkey": 20000000, "ps_suppkey": 1000000, "ps_availqty": 9999, "ps_supplycost": 99865 } },
    "nation":   { "cardinality": 25,        "avg_row_bytes": 60,  "distinct_keys": { "n_nationkey": 25, "n_regionkey": 5, "n_name": 25 } },
    "region":   { "cardinality": 5,         "avg_row_bytes": 50,  "distinct_keys": { "r_regionkey": 5, "r_name": 5 } }
}';

-- Q18: Large volume customer (customer, orders, lineitem + IN subquery)
-- No selective scan-level filters (HAVING sum > 300 is post-aggregation).
-- `c_name` and `o_orderdate` are additional `GROUP BY` keys of the outer aggregation pushed below
-- the join (see the cardinality gate in `AggregationPushdown::buildPushdownAlternative`); every
-- pushed key needs a real NDV, so both get one here (matching the SF100 magnitudes: `c_name` is
-- generated 1:1 with `c_custkey`, `o_orderdate` matches the general block above).
SET param__internal_join_table_stat_hints = '{
    "customer": { "cardinality": 15000000,  "avg_row_bytes": 25, "distinct_keys": { "c_custkey": 15000000, "c_name": 15000000 } },
    "orders":   { "cardinality": 150000000, "avg_row_bytes": 17, "distinct_keys": { "o_orderkey": 150000000, "o_custkey": 15000000, "o_totalprice": 147999998, "o_orderdate": 2406 } },
    "lineitem": { "cardinality": 600037902, "avg_row_bytes": 12, "distinct_keys": { "l_orderkey": 150000000, "l_quantity": 50 } }
}';
SELECT '-- Q18';
EXPLAIN
SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity)
FROM customer, orders, lineitem
WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING sum(l_quantity) > 300)
    AND c_custkey = o_custkey AND o_orderkey = l_orderkey
GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
ORDER BY o_totalprice DESC, o_orderdate LIMIT 100;

-- The same query with the explicit `IN` -> `JOIN` rewrite: the semi join can reorder with the
-- other joins, at the price of a second full `lineitem` aggregation on the probe side.
SELECT '-- Q18 rewrite_in_to_join';
EXPLAIN
SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity)
FROM customer, orders, lineitem
WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING sum(l_quantity) > 300)
    AND c_custkey = o_custkey AND o_orderkey = l_orderkey
GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
ORDER BY o_totalprice DESC, o_orderdate LIMIT 100
SETTINGS rewrite_in_to_join = 1;

-- Regression canary: the gate does let a genuinely shrinking pushdown through at SF100 scale.
-- Restore the general SF100 hints above - `Q18`'s block narrowed them to just its own columns.
SET param__internal_join_table_stat_hints = '{
    "lineitem": { "cardinality": 600037902, "avg_row_bytes": 128, "distinct_keys": { "l_orderkey": 150000000, "l_partkey": 20000000, "l_suppkey": 1000000, "l_linenumber": 7, "l_returnflag": 3, "l_linestatus": 2, "l_shipdate": 2526, "l_commitdate": 2466, "l_receiptdate": 2554, "l_quantity": 50, "l_discount": 11, "l_shipmode": 7, "l_shipinstruct": 4 } },
    "orders":   { "cardinality": 150000000, "avg_row_bytes": 80,  "distinct_keys": { "o_orderkey": 150000000, "o_custkey": 15000000, "o_orderdate": 2406, "o_orderstatus": 3, "o_orderpriority": 5, "o_clerk": 1000 } },
    "customer": { "cardinality": 15000000,  "avg_row_bytes": 120, "distinct_keys": { "c_custkey": 15000000, "c_nationkey": 25, "c_mktsegment": 5, "c_acctbal": 14975000, "c_phone": 14999997 } },
    "part":     { "cardinality": 20000000,  "avg_row_bytes": 90,  "distinct_keys": { "p_partkey": 20000000, "p_type": 150, "p_brand": 25, "p_size": 50, "p_container": 40, "p_name": 19999999 } },
    "supplier": { "cardinality": 1000000,   "avg_row_bytes": 110, "distinct_keys": { "s_suppkey": 1000000, "s_nationkey": 25, "s_acctbal": 999990 } },
    "partsupp": { "cardinality": 80000000,  "avg_row_bytes": 40,  "distinct_keys": { "ps_partkey": 20000000, "ps_suppkey": 1000000, "ps_availqty": 9999, "ps_supplycost": 99865 } },
    "nation":   { "cardinality": 25,        "avg_row_bytes": 60,  "distinct_keys": { "n_nationkey": 25, "n_regionkey": 5, "n_name": 25 } },
    "region":   { "cardinality": 5,         "avg_row_bytes": 50,  "distinct_keys": { "r_regionkey": 5, "r_name": 5 } }
}';

-- `c_nationkey` is both the join key and the sole `GROUP BY` key, so the pushed keys are not
-- extended by the join condition (see the condition-extension step in
-- `AggregationPushdown::buildPushdownAlternative`): composite NDV = 25 (`customer.c_nationkey`,
-- itself a foreign key into the 25-row `nation` table, so genuinely low-cardinality even though
-- `customer` is huge), input estimate = 15,000,000 (`customer`'s SF100 cardinality). Gate:
-- 25 * 2 = 50 <= 15,000,000 - comfortably (by 5-6 orders of magnitude) over the required 2x.
SELECT '-- customer distribution by nation: pushed (variant A) under the same SF100 hints';
EXPLAIN
SELECT c_nationkey, count() AS num_customers, sum(c_acctbal) AS total_balance
FROM customer JOIN nation ON c_nationkey = n_nationkey
GROUP BY c_nationkey
ORDER BY c_nationkey;

DROP TABLE lineitem;
DROP TABLE orders;
DROP TABLE customer;
DROP TABLE partsupp;
DROP TABLE supplier;
DROP TABLE part;
DROP TABLE nation;
DROP TABLE region;
