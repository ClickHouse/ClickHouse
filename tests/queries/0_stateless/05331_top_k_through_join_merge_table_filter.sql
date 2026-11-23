-- `topKThroughJoin` probes the child plans of a `Merge` read in the first optimization pass, before
-- the filters are applied to the read. A condition on `_table` must still exclude the other tables
-- from the read: `topKThroughJoin` drops the child plans its probe created, and they are created again later.
--
-- The settings below are pinned for the same reason as in 05193_top_k_through_join_final_reverse_order.

SET query_plan_top_k_through_join = 1;
SET optimize_read_in_order = 1;
SET query_plan_read_in_order_through_join = 1;
SET query_plan_join_swap_table = false;
SET query_plan_max_limit_for_top_k_optimization = 0;
SET enable_join_runtime_filters = 0;
SET enable_lazy_columns_replication = 0;
SET query_plan_optimize_lazy_materialization = 0;
SET enable_parallel_replicas = 0;
SET max_bytes_before_external_join = 0;
SET max_bytes_ratio_before_external_join = 0;

DROP TABLE IF EXISTS t_merge_filter_merge;
DROP TABLE IF EXISTS t_merge_filter_a;
DROP TABLE IF EXISTS t_merge_filter_b;
DROP TABLE IF EXISTS t_merge_filter_right;
DROP TABLE IF EXISTS t_merge_filter_with_bad_type;
DROP TABLE IF EXISTS t_merge_filter_with_bad_view;
DROP TABLE IF EXISTS t_merge_filter_bad_type;
DROP TABLE IF EXISTS t_merge_filter_bad_view;
DROP TABLE IF EXISTS t_merge_filter_dropped;

CREATE TABLE t_merge_filter_a (k Int64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_merge_filter_b (k Int64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_merge_filter_right (k Int64, v Int64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_merge_filter_merge (k Int64) ENGINE = Merge(currentDatabase(), '^t_merge_filter_[ab]$');

INSERT INTO t_merge_filter_a SELECT number FROM numbers(100);
INSERT INTO t_merge_filter_b SELECT number + 1000 FROM numbers(100);
INSERT INTO t_merge_filter_right SELECT number, number * 10 FROM numbers(2000);

-- Only `t_merge_filter_a` is read.
SELECT countIf(explain LIKE '%ReadFromMergeTree%t_merge_filter_a%'), countIf(explain LIKE '%ReadFromMergeTree%t_merge_filter_b%')
FROM ( EXPLAIN actions = 0
    SELECT l.k, r.v FROM t_merge_filter_merge AS l LEFT JOIN t_merge_filter_right AS r ON r.k = l.k
    WHERE l._table = 't_merge_filter_a'
    ORDER BY l.k DESC LIMIT 3
);

SELECT l.k, r.v FROM t_merge_filter_merge AS l LEFT JOIN t_merge_filter_right AS r ON r.k = l.k
WHERE l._table = 't_merge_filter_a'
ORDER BY l.k DESC LIMIT 3;

-- A table excluded by the condition is not planned at all, so it cannot fail the query: the type of `k` in
-- `t_merge_filter_bad_type` cannot be converted to the one of the `Merge` table, `t_merge_filter_bad_view` reads a dropped table.
CREATE TABLE t_merge_filter_bad_type (k Array(Int64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_merge_filter_bad_type VALUES ([1, 2]);
CREATE TABLE t_merge_filter_dropped (k Int64) ENGINE = MergeTree ORDER BY k;
CREATE VIEW t_merge_filter_bad_view AS SELECT k FROM t_merge_filter_dropped;
DROP TABLE t_merge_filter_dropped;
CREATE TABLE t_merge_filter_with_bad_type (k Int64) ENGINE = Merge(currentDatabase(), '^t_merge_filter_(a|bad_type)$');
CREATE TABLE t_merge_filter_with_bad_view (k Int64) ENGINE = Merge(currentDatabase(), '^t_merge_filter_(a|bad_view)$');

-- Still read in order through the join: no second `Sorting`.
SELECT countIf(explain LIKE '%Sorting%'), countIf(explain LIKE '%ReadFromMergeTree%t_merge_filter_bad_type%')
FROM ( EXPLAIN actions = 0
    SELECT l.k, r.v FROM t_merge_filter_with_bad_type AS l LEFT JOIN t_merge_filter_right AS r ON r.k = l.k
    WHERE l._table = 't_merge_filter_a'
    ORDER BY l.k DESC LIMIT 3
);

SELECT l.k, r.v FROM t_merge_filter_with_bad_type AS l LEFT JOIN t_merge_filter_right AS r ON r.k = l.k
WHERE l._table = 't_merge_filter_a'
ORDER BY l.k DESC LIMIT 3;

SELECT l.k, r.v FROM t_merge_filter_with_bad_type AS l LEFT JOIN t_merge_filter_right AS r ON r.k = l.k
WHERE l._table = 't_merge_filter_a'
ORDER BY l.k ASC LIMIT 3;

SELECT l.k, r.v FROM t_merge_filter_with_bad_view AS l LEFT JOIN t_merge_filter_right AS r ON r.k = l.k
WHERE l._table = 't_merge_filter_a'
ORDER BY l.k DESC LIMIT 3;

DROP TABLE t_merge_filter_with_bad_type;
DROP TABLE t_merge_filter_with_bad_view;
DROP TABLE t_merge_filter_bad_type;
DROP TABLE t_merge_filter_bad_view;

DROP TABLE t_merge_filter_merge;
DROP TABLE t_merge_filter_a;
DROP TABLE t_merge_filter_b;
DROP TABLE t_merge_filter_right;
