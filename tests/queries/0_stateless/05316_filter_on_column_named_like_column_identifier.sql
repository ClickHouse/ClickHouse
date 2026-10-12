-- Tags: no-fasttest, no-parallel, use-rocksdb, no-parallel-replicas
-- Tag no-parallel: another test's SYSTEM CLEAR QUERY CONDITION CACHE would remove the entry the repeated JOIN must hit.
-- A filter on a column named like another column's qualified name (`__table1.k` next to `k`) must use that column in
-- primary key, partition, skip index, PREWHERE, row policy, query condition cache and storage key analysis.

DROP TABLE IF EXISTS t_dotted;
CREATE TABLE t_dotted (k UInt64, `__table1.k` UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 128;
INSERT INTO t_dotted SELECT number, 2000 - number FROM numbers(2000);
SELECT count(), sum(k) FROM t_dotted WHERE `__table1.k` IN (1, 2);
SELECT count(), sum(k) FROM t_dotted PREWHERE `__table1.k` IN (1, 2);
SELECT count(), sum(q.k) FROM t_dotted AS q WHERE q.`__table1.k` IN (1, 2);
-- The primary key is not used for `__table1.k`, and is still used for `k`.
SELECT count(), sum(k) FROM t_dotted WHERE `__table1.k` IN (1, 2) SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }
SELECT count(), sum(k) FROM t_dotted WHERE k IN (1, 2) SETTINGS force_primary_key = 1;

DROP ROW POLICY IF EXISTS p_dotted ON t_dotted;
CREATE ROW POLICY p_dotted ON t_dotted USING `__table1.k` IN (1, 2) TO ALL;
SELECT count(), sum(k) FROM t_dotted;
DROP ROW POLICY p_dotted ON t_dotted;

DROP TABLE IF EXISTS t_part;
CREATE TABLE t_part (k UInt64, `__table1.k` UInt64) ENGINE = MergeTree PARTITION BY intDiv(k, 1000) ORDER BY tuple();
INSERT INTO t_part SELECT number, 2000 - number FROM numbers(2000);
SELECT count(), sum(k) FROM t_part WHERE `__table1.k` IN (1, 2);
-- The partition key is not used for `__table1.k`, and is still used for `k`.
SELECT count(), sum(k) FROM t_part WHERE `__table1.k` IN (1, 2) SETTINGS force_index_by_date = 1; -- { serverError INDEX_NOT_USED }
SELECT count(), sum(k) FROM t_part WHERE k IN (1, 2) SETTINGS force_index_by_date = 1;

DROP TABLE IF EXISTS t_skip;
CREATE TABLE t_skip (k UInt64, `__table1.k` UInt64, INDEX ik k TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 128;
INSERT INTO t_skip SELECT number, 2000 - number FROM numbers(2000);
SELECT count(), sum(k) FROM t_skip WHERE `__table1.k` IN (1, 2);
-- The skip index is not used for `__table1.k`, and is still used for `k`.
SELECT count(), sum(k) FROM t_skip WHERE `__table1.k` IN (1, 2) SETTINGS force_data_skipping_indices = 'ik'; -- { serverError INDEX_NOT_USED }
SELECT count(), sum(k) FROM t_skip WHERE k IN (1, 2) SETTINGS force_data_skipping_indices = 'ik';

DROP TABLE IF EXISTS t_final;
CREATE TABLE t_final (k UInt64, `__table1.k` UInt64) ENGINE = ReplacingMergeTree ORDER BY k SETTINGS index_granularity = 128;
INSERT INTO t_final SELECT number, 2000 - number FROM numbers(2000);
SELECT count(), sum(k) FROM t_final FINAL WHERE `__table1.k` IN (1, 2);

DROP TABLE IF EXISTS t_tuple;
CREATE TABLE t_tuple (k UInt64, `__table1` Tuple(k UInt64)) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 128;
INSERT INTO t_tuple SELECT number, tuple(2000 - number) FROM numbers(2000);
SELECT count(), sum(k) FROM t_tuple WHERE `__table1`.k IN (1, 2);

DROP TABLE IF EXISTS t_hint;
CREATE TABLE t_hint (k UInt64, `__table1.k` UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 128, index_granularity_bytes = '10Mi', add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_hint SELECT number, 2000 - number FROM numbers(2000);
-- `indexHint(k ...)` still prunes by `k`, also when `__table1.k` is read.
SELECT sum(k), sum(`__table1.k`) FROM t_hint WHERE indexHint(k IN (1, 2));
SELECT sum(k) FROM t_hint WHERE indexHint(k IN (1, 2));

DROP TABLE IF EXISTS t_rocksdb;
CREATE TABLE t_rocksdb (k UInt64, `__table1.k` UInt64) ENGINE = EmbeddedRocksDB PRIMARY KEY k;
INSERT INTO t_rocksdb SELECT number, 2000 - number FROM numbers(2000);
SELECT count(), sum(k) FROM t_rocksdb WHERE `__table1.k` IN (1, 2);
-- A full scan for `__table1.k`, a key lookup for `k`.
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT count(), sum(k) FROM t_rocksdb WHERE `__table1.k` IN (1, 2)) WHERE explain LIKE '%ReadType%';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT count(), sum(k) FROM t_rocksdb WHERE k IN (1, 2)) WHERE explain LIKE '%ReadType%';

DROP TABLE IF EXISTS t_qcc;
DROP TABLE IF EXISTS t_qcc_build;
CREATE TABLE t_qcc (k UInt64, `__table1.k` UInt64, pad String) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 128, add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_qcc SELECT number, 20000 - number, repeat('x', 16) FROM numbers(20000);
CREATE TABLE t_qcc_build (k UInt64) ENGINE = Memory;
INSERT INTO t_qcc_build VALUES (1), (2), (19998), (19999);
-- A JOIN filter on `__table1.k` must not write a query condition cache entry that a later filter on `k` reads.
SELECT count() FROM t_qcc AS a INNER JOIN t_qcc_build AS b ON a.k = b.k WHERE a.`__table1.k` IN (1, 2) AND b.k IN (1, 2) SETTINGS use_query_condition_cache = 1, enable_join_runtime_filters = 0, optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0, join_algorithm = 'hash', query_plan_join_swap_table = 0, enable_parallel_replicas = 0 FORMAT Null;
-- The same JOIN again finds that entry, so the cache is in use.
SELECT count() FROM t_qcc AS a INNER JOIN t_qcc_build AS b ON a.k = b.k WHERE a.`__table1.k` IN (1, 2) AND b.k IN (1, 2) SETTINGS use_query_condition_cache = 1, enable_join_runtime_filters = 0, optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0, join_algorithm = 'hash', query_plan_join_swap_table = 0, enable_parallel_replicas = 0, log_comment = '05316_qcc_writer_again' FORMAT Null;
SELECT count() FROM t_qcc WHERE k IN (1, 2) AND k IN (1, 2) SETTINGS use_query_condition_cache = 1, enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, enable_join_runtime_filters_index_analysis = 0, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, enable_multiple_prewhere_read_steps = 1, join_algorithm = 'hash', query_plan_join_swap_table = 0, enable_parallel_replicas = 0;
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['QueryConditionCacheHits'] > 0 FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05316_qcc_writer_again';

DROP TABLE t_dotted;
DROP TABLE t_part;
DROP TABLE t_skip;
DROP TABLE t_final;
DROP TABLE t_tuple;
DROP TABLE t_hint;
DROP TABLE t_rocksdb;
DROP TABLE t_qcc;
DROP TABLE t_qcc_build;
