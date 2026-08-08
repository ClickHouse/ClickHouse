-- Reading in reverse order with FINAL behind a JOIN.
--
-- Two optimizations compete for `... FINAL LEFT JOIN ... ORDER BY key DESC LIMIT n`:
-- `topKThroughJoin` (injects a Sort + Limit on the preserved input) and the second-pass
-- read-in-order through the join. `topKThroughJoin` defers to the second pass only when the
-- second pass can actually install read-in-order, which for a reverse direction with FINAL
-- requires a storage that supports it (`ReplacingMergeTree` with
-- `optimize_read_in_reverse_order_final`). Otherwise it must fire itself, so that the query
-- gets at least one of the two optimizations.
--
-- Beyond the plan gates, this checks that the rows selected by FINAL are the same whichever
-- optimization applies: the reverse reading order must not change which row of a duplicate
-- key group survives.
--
-- The settings pinned in the plan queries mirror 04209_top_k_through_join_read_in_order_gate:
-- the stateless test runner randomizes them, and each of them can defeat the deferral check.

SET enable_analyzer = 1;
SET query_plan_top_k_through_join = 1;

DROP TABLE IF EXISTS t_replacing;
DROP TABLE IF EXISTS t_aggregating;
DROP TABLE IF EXISTS t_version;
DROP TABLE IF EXISTS t_deleted;
DROP TABLE IF EXISTS t_desc_key;
DROP TABLE IF EXISTS t_right;

CREATE TABLE t_right (k Int64, v Int64) ENGINE = MergeTree() ORDER BY k;
INSERT INTO t_right SELECT number, number * 10 FROM numbers(1000);

-- `src` records which INSERT a row came from, so the reference pins down which row of a
-- duplicate key group FINAL selected.
CREATE TABLE t_replacing (k Int64, src Int64) ENGINE = ReplacingMergeTree() ORDER BY k;
INSERT INTO t_replacing SELECT number, 1 FROM numbers(1000);
-- Overlapping tail in a newer part: for k >= 990 the newer part wins.
INSERT INTO t_replacing SELECT number, 2 FROM numbers(990, 10);
-- Duplicates inside a single level-0 part: the row written last wins, in both reading directions.
INSERT INTO t_replacing SETTINGS optimize_on_insert = 0 VALUES (999, 3), (999, 4);

CREATE TABLE t_aggregating (k Int64, src Int64) ENGINE = AggregatingMergeTree() ORDER BY k;
INSERT INTO t_aggregating SELECT number, 1 FROM numbers(1000);

-- Max version wins; among equal versions the row from the newest part wins.
CREATE TABLE t_version (k Int64, src Int64, ver Int64) ENGINE = ReplacingMergeTree(ver) ORDER BY k;
INSERT INTO t_version SELECT number, 1, number FROM numbers(1000);
INSERT INTO t_version SELECT number, 2, 0 FROM numbers(995, 5);
INSERT INTO t_version SETTINGS optimize_on_insert = 0 VALUES (999, 3, 999), (998, 4, 5000);

CREATE TABLE t_deleted (k Int64, src Int64, ver Int64, is_deleted UInt8) ENGINE = ReplacingMergeTree(ver, is_deleted) ORDER BY k;
INSERT INTO t_deleted SELECT number, 1, 1, 0 FROM numbers(1000);
INSERT INTO t_deleted SETTINGS optimize_on_insert = 0 VALUES (999, 2, 2, 1), (997, 3, 2, 1);

-- Descending sorting key: the storage order is already descending, so `ORDER BY k DESC` is the
-- direct reading direction for it even though the sort description says descending.
CREATE TABLE t_desc_key (k Int64, src Int64) ENGINE = ReplacingMergeTree() ORDER BY k DESC;
INSERT INTO t_desc_key SELECT number, 1 FROM numbers(1000);
INSERT INTO t_desc_key SETTINGS optimize_on_insert = 0 VALUES (999, 2), (999, 3);

-- Plan gates.

-- ReplacingMergeTree with the optimization on: the second pass can read in reverse order with
-- FINAL, so defer to it and do not inject an inner Sort + Limit.
SELECT 'plan_replacing_reverse_on' AS label, countIf(explain LIKE '%Sorting%') AS sort_count, countIf(explain LIKE '%Limit%') AS limit_count
FROM ( EXPLAIN actions = 0
    SELECT l.k, r.v FROM t_replacing AS l FINAL LEFT JOIN t_right AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 10
    SETTINGS optimize_read_in_order = 1, optimize_read_in_reverse_order_final = 1,
             query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
             query_plan_join_swap_table = false, query_plan_max_limit_for_top_k_optimization = 0,
             enable_join_runtime_filters = 0, enable_lazy_columns_replication = 0,
             query_plan_optimize_lazy_materialization = 0, enable_parallel_replicas = 0,
             max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
);

-- Same table with the optimization off: the second pass rejects the reverse direction with
-- FINAL, so `topKThroughJoin` must fire and add its own Sort + Limit.
SELECT 'plan_replacing_reverse_off' AS label, countIf(explain LIKE '%Sorting%') AS sort_count, countIf(explain LIKE '%Limit%') AS limit_count
FROM ( EXPLAIN actions = 0
    SELECT l.k, r.v FROM t_replacing AS l FINAL LEFT JOIN t_right AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 10
    SETTINGS optimize_read_in_order = 1, optimize_read_in_reverse_order_final = 0,
             query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
             query_plan_join_swap_table = false, query_plan_max_limit_for_top_k_optimization = 0,
             enable_join_runtime_filters = 0, enable_lazy_columns_replication = 0,
             query_plan_optimize_lazy_materialization = 0, enable_parallel_replicas = 0,
             max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
);

-- An engine that does not support reading in reverse order with FINAL: `topKThroughJoin` fires.
SELECT 'plan_aggregating_reverse' AS label, countIf(explain LIKE '%Sorting%') AS sort_count, countIf(explain LIKE '%Limit%') AS limit_count
FROM ( EXPLAIN actions = 0
    SELECT l.k, r.v FROM t_aggregating AS l FINAL LEFT JOIN t_right AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 10
    SETTINGS optimize_read_in_order = 1, optimize_read_in_reverse_order_final = 1,
             query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
             query_plan_join_swap_table = false, query_plan_max_limit_for_top_k_optimization = 0,
             enable_join_runtime_filters = 0, enable_lazy_columns_replication = 0,
             query_plan_optimize_lazy_materialization = 0, enable_parallel_replicas = 0,
             max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
);

-- Sorting in the direct order defers regardless of the setting, as before.
SELECT 'plan_replacing_direct' AS label, countIf(explain LIKE '%Sorting%') AS sort_count, countIf(explain LIKE '%Limit%') AS limit_count
FROM ( EXPLAIN actions = 0
    SELECT l.k, r.v FROM t_replacing AS l FINAL LEFT JOIN t_right AS r ON r.k = l.k
    ORDER BY l.k ASC LIMIT 10
    SETTINGS optimize_read_in_order = 1, optimize_read_in_reverse_order_final = 0,
             query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
             query_plan_join_swap_table = false, query_plan_max_limit_for_top_k_optimization = 0,
             enable_join_runtime_filters = 0, enable_lazy_columns_replication = 0,
             query_plan_optimize_lazy_materialization = 0, enable_parallel_replicas = 0,
             max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
);

-- Plan gates for a descending sorting key, where the reading direction is the sort description's
-- direction flipped by the key's reverse flag. Counting the Sort + Limit pairs alone cannot tell
-- a sound deferral from one where the second pass rejects the read afterwards (both leave only
-- the outer pair), so these also count the reading types: the deferral is only correct when it
-- ends in an in-order or reverse-order read of the left table.
SELECT 'plan_desc_key_asc_sort_on' AS label,
       countIf(explain LIKE '%Sorting%') AS sort_count,
       countIf(explain LIKE '%InReverseOrder%') AS reverse_reads,
       countIf(explain LIKE '%: InOrder%') AS direct_reads
FROM ( EXPLAIN actions = 1
    SELECT l.k, r.v FROM t_desc_key AS l FINAL LEFT JOIN t_right AS r ON r.k = l.k
    ORDER BY l.k ASC LIMIT 10
    SETTINGS optimize_read_in_order = 1, optimize_read_in_reverse_order_final = 1,
             query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
             query_plan_join_swap_table = false, query_plan_max_limit_for_top_k_optimization = 0,
             enable_join_runtime_filters = 0, enable_lazy_columns_replication = 0,
             query_plan_optimize_lazy_materialization = 0, enable_parallel_replicas = 0,
             max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
);

-- The same shape with the optimization off: the second pass would reject the reverse read, so
-- `topKThroughJoin` must fire instead of deferring into a plan with no optimization at all.
SELECT 'plan_desc_key_asc_sort_off' AS label,
       countIf(explain LIKE '%Sorting%') AS sort_count,
       countIf(explain LIKE '%InReverseOrder%') AS reverse_reads,
       countIf(explain LIKE '%: InOrder%') AS direct_reads
FROM ( EXPLAIN actions = 1
    SELECT l.k, r.v FROM t_desc_key AS l FINAL LEFT JOIN t_right AS r ON r.k = l.k
    ORDER BY l.k ASC LIMIT 10
    SETTINGS optimize_read_in_order = 1, optimize_read_in_reverse_order_final = 0,
             query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
             query_plan_join_swap_table = false, query_plan_max_limit_for_top_k_optimization = 0,
             enable_join_runtime_filters = 0, enable_lazy_columns_replication = 0,
             query_plan_optimize_lazy_materialization = 0, enable_parallel_replicas = 0,
             max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
);

-- A descending sort description of a descending sorting key is the direct reading direction, so
-- the deferral is sound even with the optimization off.
SELECT 'plan_desc_key_desc_sort_off' AS label,
       countIf(explain LIKE '%Sorting%') AS sort_count,
       countIf(explain LIKE '%InReverseOrder%') AS reverse_reads,
       countIf(explain LIKE '%: InOrder%') AS direct_reads
FROM ( EXPLAIN actions = 1
    SELECT l.k, r.v FROM t_desc_key AS l FINAL LEFT JOIN t_right AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 10
    SETTINGS optimize_read_in_order = 1, optimize_read_in_reverse_order_final = 0,
             query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
             query_plan_join_swap_table = false, query_plan_max_limit_for_top_k_optimization = 0,
             enable_join_runtime_filters = 0, enable_lazy_columns_replication = 0,
             query_plan_optimize_lazy_materialization = 0, enable_parallel_replicas = 0,
             max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
);

-- Results. Each dataset is read three ways: with the reverse read-in-order optimization
-- (`reverse_on`), with `topKThroughJoin` instead (`topk`, the optimization disabled), and with
-- neither optimization (`plain`, the ground truth). All three must agree, including `src`,
-- which identifies the selected row of every duplicate key group.

SELECT 'rows_reverse_on' AS label, groupArray((k, src, v)) FROM (
    SELECT l.k AS k, l.src AS src, r.v AS v FROM t_replacing AS l FINAL LEFT JOIN t_right AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 10
    SETTINGS optimize_read_in_reverse_order_final = 1, optimize_read_in_order = 1,
             query_plan_top_k_through_join = 1, enable_parallel_replicas = 0,
             max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
);

SELECT 'rows_topk' AS label, groupArray((k, src, v)) FROM (
    SELECT l.k AS k, l.src AS src, r.v AS v FROM t_replacing AS l FINAL LEFT JOIN t_right AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 10
    SETTINGS optimize_read_in_reverse_order_final = 0, optimize_read_in_order = 1,
             query_plan_top_k_through_join = 1, enable_parallel_replicas = 0
);

SELECT 'rows_plain' AS label, groupArray((k, src, v)) FROM (
    SELECT l.k AS k, l.src AS src, r.v AS v FROM t_replacing AS l FINAL LEFT JOIN t_right AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 10
    SETTINGS optimize_read_in_order = 0, query_plan_top_k_through_join = 0,
             enable_parallel_replicas = 0
);

SELECT 'rows_version_reverse_on' AS label, groupArray((k, src, ver, v)) FROM (
    SELECT l.k AS k, l.src AS src, l.ver AS ver, r.v AS v FROM t_version AS l FINAL LEFT JOIN t_right AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 10
    SETTINGS optimize_read_in_reverse_order_final = 1, optimize_read_in_order = 1,
             query_plan_top_k_through_join = 1, enable_parallel_replicas = 0,
             max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
);

SELECT 'rows_version_plain' AS label, groupArray((k, src, ver, v)) FROM (
    SELECT l.k AS k, l.src AS src, l.ver AS ver, r.v AS v FROM t_version AS l FINAL LEFT JOIN t_right AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 10
    SETTINGS optimize_read_in_order = 0, query_plan_top_k_through_join = 0,
             enable_parallel_replicas = 0
);

SELECT 'rows_deleted_reverse_on' AS label, groupArray((k, src, v)) FROM (
    SELECT l.k AS k, l.src AS src, r.v AS v FROM t_deleted AS l FINAL LEFT JOIN t_right AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 10
    SETTINGS optimize_read_in_reverse_order_final = 1, optimize_read_in_order = 1,
             query_plan_top_k_through_join = 1, enable_parallel_replicas = 0,
             max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
);

SELECT 'rows_deleted_plain' AS label, groupArray((k, src, v)) FROM (
    SELECT l.k AS k, l.src AS src, r.v AS v FROM t_deleted AS l FINAL LEFT JOIN t_right AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 10
    SETTINGS optimize_read_in_order = 0, query_plan_top_k_through_join = 0,
             enable_parallel_replicas = 0
);

SELECT 'rows_desc_key_reverse_on' AS label, groupArray((k, src, v)) FROM (
    SELECT l.k AS k, l.src AS src, r.v AS v FROM t_desc_key AS l FINAL LEFT JOIN t_right AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 10
    SETTINGS optimize_read_in_reverse_order_final = 1, optimize_read_in_order = 1,
             query_plan_top_k_through_join = 1, enable_parallel_replicas = 0,
             max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
);

SELECT 'rows_desc_key_plain' AS label, groupArray((k, src, v)) FROM (
    SELECT l.k AS k, l.src AS src, r.v AS v FROM t_desc_key AS l FINAL LEFT JOIN t_right AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 10
    SETTINGS optimize_read_in_order = 0, query_plan_top_k_through_join = 0,
             enable_parallel_replicas = 0
);

-- An ascending sort of a descending sorting key is the reverse reading direction for it.
SELECT 'rows_desc_key_asc_reverse_on' AS label, groupArray((k, src, v)) FROM (
    SELECT l.k AS k, l.src AS src, r.v AS v FROM t_desc_key AS l FINAL LEFT JOIN t_right AS r ON r.k = l.k
    ORDER BY l.k ASC LIMIT 10
    SETTINGS optimize_read_in_reverse_order_final = 1, optimize_read_in_order = 1,
             query_plan_top_k_through_join = 1, enable_parallel_replicas = 0,
             max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
);

SELECT 'rows_desc_key_asc_plain' AS label, groupArray((k, src, v)) FROM (
    SELECT l.k AS k, l.src AS src, r.v AS v FROM t_desc_key AS l FINAL LEFT JOIN t_right AS r ON r.k = l.k
    ORDER BY l.k ASC LIMIT 10
    SETTINGS optimize_read_in_order = 0, query_plan_top_k_through_join = 0,
             enable_parallel_replicas = 0
);

-- An INNER JOIN is not a shape `topKThroughJoin` defers for, so the reverse read-in-order
-- optimization is what applies here; the rows must still match the unoptimized read.
SELECT 'rows_inner_reverse_on' AS label, groupArray((k, src, v)) FROM (
    SELECT l.k AS k, l.src AS src, r.v AS v FROM t_replacing AS l FINAL INNER JOIN t_right AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 10
    SETTINGS optimize_read_in_reverse_order_final = 1, optimize_read_in_order = 1,
             query_plan_top_k_through_join = 1, enable_parallel_replicas = 0,
             max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
);

SELECT 'rows_inner_plain' AS label, groupArray((k, src, v)) FROM (
    SELECT l.k AS k, l.src AS src, r.v AS v FROM t_replacing AS l FINAL INNER JOIN t_right AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 10
    SETTINGS optimize_read_in_order = 0, query_plan_top_k_through_join = 0,
             enable_parallel_replicas = 0
);

-- A limit larger than one block, so the reverse read spans several mark ranges behind the join.
SELECT 'rows_large_limit_equal' AS label, (SELECT groupArray((k, src, v)) FROM (
    SELECT l.k AS k, l.src AS src, r.v AS v FROM t_replacing AS l FINAL LEFT JOIN t_right AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 500
    SETTINGS optimize_read_in_reverse_order_final = 1, optimize_read_in_order = 1,
             query_plan_top_k_through_join = 1, enable_parallel_replicas = 0,
             max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
)) = (SELECT groupArray((k, src, v)) FROM (
    SELECT l.k AS k, l.src AS src, r.v AS v FROM t_replacing AS l FINAL LEFT JOIN t_right AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 500
    SETTINGS optimize_read_in_order = 0, query_plan_top_k_through_join = 0,
             enable_parallel_replicas = 0
));

-- A Merge table over ReplacingMergeTree children behind a join. `topKThroughJoin` never defers for
-- a Merge table, because it looks for a MergeTree read on the preserved input and a Merge table
-- reads through its own step, so it injects its Sort + Limit. The children are still read in
-- reverse order, because that injected sort is itself satisfied by reading in order, so the only
-- cost of the missing deferral is the extra sort step. With `topKThroughJoin` disabled, the second
-- pass reads the children in reverse order through the join without it.
CREATE TABLE t_merge (k Int64, src Int64) ENGINE = Merge(currentDatabase(), '^t_replacing$');

SELECT 'plan_merge_topk_on' AS label,
       countIf(explain LIKE '%Sorting%') AS sort_count,
       countIf(explain LIKE '%InReverseOrder%') AS reverse_reads
FROM ( EXPLAIN actions = 1
    SELECT l.k, r.v FROM t_merge AS l FINAL LEFT JOIN t_right AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 10
    SETTINGS query_plan_top_k_through_join = 1,
             optimize_read_in_order = 1, optimize_read_in_reverse_order_final = 1,
             query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
             query_plan_join_swap_table = false, query_plan_max_limit_for_top_k_optimization = 0,
             enable_join_runtime_filters = 0, enable_lazy_columns_replication = 0,
             query_plan_optimize_lazy_materialization = 0, enable_parallel_replicas = 0,
             max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
);

SELECT 'plan_merge_topk_off' AS label,
       countIf(explain LIKE '%Sorting%') AS sort_count,
       countIf(explain LIKE '%InReverseOrder%') AS reverse_reads
FROM ( EXPLAIN actions = 1
    SELECT l.k, r.v FROM t_merge AS l FINAL LEFT JOIN t_right AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 10
    SETTINGS query_plan_top_k_through_join = 0,
             optimize_read_in_order = 1, optimize_read_in_reverse_order_final = 1,
             query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
             query_plan_join_swap_table = false, query_plan_max_limit_for_top_k_optimization = 0,
             enable_join_runtime_filters = 0, enable_lazy_columns_replication = 0,
             query_plan_optimize_lazy_materialization = 0, enable_parallel_replicas = 0,
             max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
);

SELECT 'rows_merge_reverse_on' AS label, groupArray((k, src, v)) FROM (
    SELECT l.k AS k, l.src AS src, r.v AS v FROM t_merge AS l FINAL LEFT JOIN t_right AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 10
    SETTINGS query_plan_top_k_through_join = 0,
             optimize_read_in_reverse_order_final = 1, optimize_read_in_order = 1,
             enable_parallel_replicas = 0,
             max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
);

SELECT 'rows_merge_topk' AS label, groupArray((k, src, v)) FROM (
    SELECT l.k AS k, l.src AS src, r.v AS v FROM t_merge AS l FINAL LEFT JOIN t_right AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 10
    SETTINGS query_plan_top_k_through_join = 1,
             optimize_read_in_reverse_order_final = 1, optimize_read_in_order = 1,
             enable_parallel_replicas = 0,
             max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
);

SELECT 'rows_merge_plain' AS label, groupArray((k, src, v)) FROM (
    SELECT l.k AS k, l.src AS src, r.v AS v FROM t_merge AS l FINAL LEFT JOIN t_right AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 10
    SETTINGS optimize_read_in_order = 0, query_plan_top_k_through_join = 0,
             enable_parallel_replicas = 0
);

DROP TABLE t_merge;
DROP TABLE t_replacing;
DROP TABLE t_aggregating;
DROP TABLE t_version;
DROP TABLE t_deleted;
DROP TABLE t_desc_key;
DROP TABLE t_right;
