-- The join order optimizer counts rows only, so it joins the small bridge table with the filtered dimension
-- first and the fact table last, which shuffles the whole fact table. Cascades also counts the data moved
-- between nodes. Given a second complete join order (`cascades_join_order_alternatives`), it joins the fact
-- table with the broadcast dimension first and shuffles only that smaller result. The hints make the fact
-- table look like 100 million rows and the bridge 4 million; the filter on the dimension has a default
-- selectivity, so the bridge with the dimension is not broadcast. Only the joins and reads of the plan are
-- checked, and the result must be the same with either order and on one node.
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET param__internal_cascades_cluster_node_count = 8;
SET distributed_plan_execute_locally = 1;
SET distributed_plan_max_bytes_to_broadcast = 1048576;
SET distributed_plan_max_rows_to_broadcast = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_optimize_join_order_algorithm = 'dphyp';
SET allow_experimental_statistics = 1;
SET use_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET enable_join_runtime_filters = 0;
SET collect_hash_table_stats_during_joins = 0;
SET use_hash_table_stats_for_join_reordering = 0;
SET max_rows_to_group_by = 0;
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS t_alt_fact;
DROP TABLE IF EXISTS t_alt_bridge;
DROP TABLE IF EXISTS t_alt_dim;
DROP TABLE IF EXISTS t_alt_out;
CREATE TABLE t_alt_fact (p UInt64, s UInt64, v UInt64) ENGINE = MergeTree ORDER BY p SETTINGS auto_statistics_types = '';
CREATE TABLE t_alt_bridge (p UInt64, s UInt64) ENGINE = MergeTree ORDER BY p SETTINGS auto_statistics_types = '';
CREATE TABLE t_alt_dim (p UInt64 STATISTICS(uniq, basic), name String STATISTICS(basic)) ENGINE = MergeTree ORDER BY p SETTINGS auto_statistics_types = '';
INSERT INTO t_alt_fact SELECT number % 400, intDiv(number, 400) % 4, number FROM numbers(16000);
INSERT INTO t_alt_bridge SELECT intDiv(number, 4), number % 4 FROM numbers(1600);
INSERT INTO t_alt_dim SELECT number, if(number < 100, 'keep', 'skip') FROM numbers(1000);
CREATE TABLE t_alt_out (p UInt64, w UInt64) ENGINE = MergeTree ORDER BY w SETTINGS auto_statistics_types = '';
INSERT INTO t_alt_out SELECT number, number % 10 FROM numbers(20);

SET param__internal_join_table_stat_hints = '{"t_alt_fact": {"cardinality": 100000000, "avg_row_bytes": 128, "distinct_keys": {"p": 1000000, "s": 10000}}, "t_alt_bridge": {"cardinality": 4000000, "avg_row_bytes": 16, "distinct_keys": {"p": 1000000, "s": 10000}}}';

SELECT '-- the fact table is joined with the broadcast dimension before any shuffle';
SELECT trimLeft(explain) FROM (EXPLAIN SELECT sum(f.v) FROM t_alt_fact AS f JOIN t_alt_bridge AS b ON f.p = b.p AND f.s = b.s JOIN t_alt_dim AS d ON b.p = d.p WHERE d.name LIKE '%keep%'
    -- The fact table and the dimension are joined only through the bridge's key, so the order that joins
    -- them first needs the transitive predicate; the runner may turn that off, and may fix the join sides.
    SETTINGS enable_join_transitive_predicates = 1, query_plan_join_swap_table = 'auto')
WHERE explain LIKE '%JoinLogical%' OR explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Exchange%';
SELECT '-- the same result with either order and on one node';
SELECT sum(f.v), count() FROM t_alt_fact AS f JOIN t_alt_bridge AS b ON f.p = b.p AND f.s = b.s JOIN t_alt_dim AS d ON b.p = d.p WHERE d.name LIKE '%keep%';
SELECT sum(f.v), count() FROM t_alt_fact AS f JOIN t_alt_bridge AS b ON f.p = b.p AND f.s = b.s JOIN t_alt_dim AS d ON b.p = d.p WHERE d.name LIKE '%keep%' SETTINGS cascades_join_order_alternatives = 0;
SELECT sum(f.v), count() FROM t_alt_fact AS f JOIN t_alt_bridge AS b ON f.p = b.p AND f.s = b.s JOIN t_alt_dim AS d ON b.p = d.p WHERE d.name LIKE '%keep%' SETTINGS make_distributed_plan = 0;

-- The same three tables inside a correlated subquery: the join with the outer table builds a runtime
-- filter, which must reach the reads of both join orders.
SELECT '-- the runtime filter from the outer table reaches the reads inside the subquery';
SELECT if(explain LIKE '%Runtime filters%',
        if(explain LIKE '%from ' || currentDatabase() || '.t_alt_out)%', 'filtered by t_alt_out', 'not filtered by t_alt_out'),
        replaceAll(replaceRegexpOne(explain, '^[^A-Za-z]+', ''), currentDatabase() || '.', '')) FROM (EXPLAIN SELECT count() FROM t_alt_out AS o WHERE o.w <= (
        SELECT max(f.v) FROM t_alt_fact AS f JOIN t_alt_bridge AS b ON f.p = b.p AND f.s = b.s JOIN t_alt_dim AS d ON b.p = d.p
        WHERE d.name LIKE '%keep%' AND f.p = o.p)
    SETTINGS enable_join_transitive_predicates = 1, query_plan_join_swap_table = 'auto', enable_join_runtime_filters = 1,
        join_runtime_filter_min_probe_rows = 0, allow_experimental_correlated_subqueries = 1,
        -- The subquery becomes an outer join, which gets no runtime filter unless it is converted to an inner one.
        query_plan_convert_outer_join_to_inner_join = 1,
        -- Shows the runtime filters in the reads instead of in separate filter steps.
        optimize_move_to_prewhere = 1,
        correlated_subqueries_use_in_memory_buffer = 0, explain_query_plan_default = 'pretty')
WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Runtime filters%';
SELECT count() FROM t_alt_out AS o WHERE o.w <= (
    SELECT max(f.v) FROM t_alt_fact AS f JOIN t_alt_bridge AS b ON f.p = b.p AND f.s = b.s JOIN t_alt_dim AS d ON b.p = d.p
    WHERE d.name LIKE '%keep%' AND f.p = o.p)
SETTINGS enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, allow_experimental_correlated_subqueries = 1,
    -- DPhyp alone does not order outer joins.
    query_plan_convert_outer_join_to_inner_join = 1;

DROP TABLE t_alt_fact;
DROP TABLE t_alt_bridge;
DROP TABLE t_alt_dim;
DROP TABLE t_alt_out;
