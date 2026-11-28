-- Tags: no-fasttest, no-replicated-database
-- Tag no-replicated-database: the replicated DDL worker evaluates the `IcebergLocal` arguments without the session, so it does not see the `TEMPORARY TABLE iceberg_path`

SET allow_insert_into_iceberg = 1, allow_experimental_iceberg_compaction = 0, use_iceberg_metadata_files_cache = 0, iceberg_delete_data_on_drop = 0, async_insert = 0;
-- One stream, so the files are read one after another; the in-order optimizations are on.
SET max_threads = 1, optimize_read_in_order = 1, optimize_aggregation_in_order = 1, optimize_distinct_in_order = 1;

CREATE TEMPORARY TABLE iceberg_path AS
WITH if(changed, trimBoth(value), 'user_files/') AS user_files_path
SELECT concat(
    if(startsWith(user_files_path, '/'), '', (SELECT path FROM system.disks WHERE name = 'default')),
    user_files_path, '/', currentDatabase(), '/05367_merge_read_in_order_iceberg/') AS path
FROM system.server_settings WHERE name = 'user_files_path';

CREATE TABLE t (k Int64) ENGINE = IcebergLocal((SELECT path FROM iceberg_path), 'Parquet') ORDER BY k;
-- Three files; reading them in any order gives unsorted rows.
INSERT INTO t VALUES (1), (3), (5);
INSERT INTO t VALUES (0), (2), (4);
INSERT INTO t VALUES (1), (3), (5);
SELECT count() FROM t; -- a direct read refreshes the snapshot the `Merge` read uses

SELECT groupArray(k) FROM (SELECT k FROM merge(currentDatabase(), '^t$') ORDER BY k);
SELECT groupArray(k) FROM (SELECT k FROM merge(currentDatabase(), '^t$') ORDER BY k DESC);
SELECT groupArray(k) FROM (SELECT k FROM merge(currentDatabase(), '^t$') ORDER BY k LIMIT 4);
SELECT groupArray(k) FROM (SELECT DISTINCT k FROM merge(currentDatabase(), '^t$') ORDER BY k);
SELECT count() FROM (SELECT k FROM merge(currentDatabase(), '^t$') LIMIT 1 BY k);
SELECT groupArray((k, c)) FROM (SELECT k, count() AS c FROM merge(currentDatabase(), '^t$') GROUP BY k ORDER BY k);

CREATE TABLE m (k Int64) ENGINE = MergeTree ORDER BY k;
INSERT INTO m VALUES (6), (7);
SELECT groupArray(k) FROM (SELECT k FROM merge(currentDatabase(), '^(t|m)$') ORDER BY k);
-- Neither direction leaves the `MergeTree` child reading in order when the `Merge` read declines.
SELECT countIf(explain LIKE '%ReadType: In%') FROM (EXPLAIN PLAN actions = 1 SELECT k FROM merge(currentDatabase(), '^(t|m)$') ORDER BY k) SETTINGS explain_query_plan_default = 'legacy', enable_parallel_replicas = 0;
SELECT countIf(explain LIKE '%ReadType: In%') FROM (EXPLAIN PLAN actions = 1 SELECT k FROM merge(currentDatabase(), '^(t|m)$') ORDER BY k DESC) SETTINGS explain_query_plan_default = 'legacy', enable_parallel_replicas = 0;
-- `topKThroughJoin` does not defer to a read in order that the `Merge` read declines: it adds its own Sort + Limit.
SELECT countIf(explain LIKE '%Sorting%'), countIf(explain LIKE '%Limit%')
FROM (EXPLAIN actions = 0
    SELECT l.k FROM merge(currentDatabase(), '^t$') AS l LEFT JOIN m AS r ON r.k = l.k ORDER BY l.k LIMIT 3)
SETTINGS query_plan_top_k_through_join = 1, query_plan_read_in_order_through_join = 1,
    query_plan_join_swap_table = false, query_plan_max_limit_for_top_k_optimization = 0,
    enable_join_runtime_filters = 0, enable_lazy_columns_replication = 0, query_plan_optimize_lazy_materialization = 0,
    enable_parallel_replicas = 0, max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SELECT groupArray(k) FROM (
    SELECT l.k AS k FROM merge(currentDatabase(), '^t$') AS l LEFT JOIN m AS r ON r.k = l.k ORDER BY l.k LIMIT 3
    SETTINGS query_plan_top_k_through_join = 1, query_plan_read_in_order_through_join = 1,
        query_plan_join_swap_table = false, query_plan_max_limit_for_top_k_optimization = 0,
        enable_join_runtime_filters = 0, enable_lazy_columns_replication = 0, query_plan_optimize_lazy_materialization = 0,
        enable_parallel_replicas = 0, max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0);

DROP TABLE t;
DROP TABLE m;
