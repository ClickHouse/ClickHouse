-- Test for enable_join_seal_gated_reading: the probe side of a hash JOIN is read through
-- SealGatedReadTransforms which start reading only after the build side completes its runtime
-- filter, and the filter then prunes whole mark ranges by the primary key at task-cut time.

-- Runtime filters (and therefore the gating) exist only with the analyzer.
SET enable_analyzer = 1;
SET enable_join_runtime_filters = 1;
SET enable_join_seal_gated_reading = 1;
-- The gating marks only plain local reads (no parallel replicas).
SET enable_parallel_replicas = 0;
-- Pin the default multi-threaded read pool for the main queries; single-stream, in-order
-- and prefetched-pool reads are gated too and covered by dedicated queries below.
SET max_threads = 4, merge_tree_min_rows_for_concurrent_read = 256, optimize_read_in_order = 0;
SET allow_prefetched_read_pool_for_local_filesystem = 0, allow_prefetched_read_pool_for_remote_filesystem = 0;
-- Keep the read-time runtime-filter index analysis out of the picture: it prunes granules on
-- its own (also for the ungated reference query), which would break the read_rows contrast.
SET enable_join_runtime_filters_index_analysis = 0, use_skip_indexes_on_data_read = 0;
-- The asserts need the big table on the probe side: keep the join order deterministic (the
-- CI randomization can flip the sides, which correctly falls back to ungated reading).
SET query_plan_optimize_join_order_randomize = 0, query_plan_join_swap_table = 'false';
-- A cached hash-table size hint can make an exact-set-only key (e.g. String) eligible for
-- gating once a previous run recorded the build size: keep the decisions run-independent.
SET join_runtime_filter_size_from_hash_table_stats = 0;

DROP TABLE IF EXISTS t_seal_probe;
DROP TABLE IF EXISTS t_seal_build;

CREATE TABLE t_seal_probe (k UInt64, v String) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 8;
INSERT INTO t_seal_probe SELECT number, toString(number) FROM numbers(50000);
OPTIMIZE TABLE t_seal_probe FINAL;

CREATE TABLE t_seal_build (k UInt64) ENGINE = MergeTree ORDER BY k;
-- 5 keys spread over the probe primary key range: almost all of the ~6250 marks are prunable.
INSERT INTO t_seal_build SELECT number * 10000 FROM numbers(5);

-- The gated result must match the ungated one.
SELECT /* seal_gated_join */ count(), sum(p.k) FROM t_seal_probe AS p JOIN t_seal_build AS b ON p.k = b.k;
SELECT /* seal_ungated_join */ count(), sum(p.k) FROM t_seal_probe AS p JOIN t_seal_build AS b ON p.k = b.k
    SETTINGS enable_join_seal_gated_reading = 0;

-- The probe side is read through seal-gated transforms.
SELECT count() > 0 AS has_gated_reads FROM (
    EXPLAIN PIPELINE SELECT count() FROM t_seal_probe AS p JOIN t_seal_build AS b ON p.k = b.k
) WHERE explain LIKE '%SealGatedRead%';

-- The seal carries an empty key set when the build side is empty: everything is pruned.
SELECT /* seal_empty_build */ count() FROM t_seal_probe AS p JOIN t_seal_build AS b ON p.k = b.k WHERE b.k > 100500000;

-- Single-stream reading is gated too.
SELECT /* seal_gated_single_stream */ count(), sum(p.k) FROM t_seal_probe AS p JOIN t_seal_build AS b ON p.k = b.k
    SETTINGS max_threads = 1;
SELECT count() > 0 AS has_gated_reads_single_stream FROM (
    EXPLAIN PIPELINE SELECT count() FROM t_seal_probe AS p JOIN t_seal_build AS b ON p.k = b.k SETTINGS max_threads = 1
) WHERE explain LIKE '%SealGatedRead%';

-- Reading in the order of the primary key is gated too.
SELECT /* seal_gated_in_order */ p.k FROM t_seal_probe AS p JOIN t_seal_build AS b ON p.k = b.k ORDER BY p.k LIMIT 3
    SETTINGS optimize_read_in_order = 1, query_plan_read_in_order_through_join = 1;

-- Reading in the reverse order of the primary key is gated too.
SELECT /* seal_gated_reverse_order */ p.k FROM t_seal_probe AS p JOIN t_seal_build AS b ON p.k = b.k ORDER BY p.k DESC LIMIT 3
    SETTINGS optimize_read_in_order = 1, query_plan_read_in_order_through_join = 1;

-- The prefetched read pool is gated too, and no prefetch is issued for dropped ranges.
SELECT /* seal_gated_prefetched */ count(), sum(p.k) FROM t_seal_probe AS p JOIN t_seal_build AS b ON p.k = b.k
    SETTINGS allow_prefetched_read_pool_for_local_filesystem = 1, local_filesystem_read_method = 'pread_threadpool';

-- Ungatable reads fall back to plain reading with row-level filtering (correct results):
-- parallel replicas (the seal cannot cross replicas)...
SELECT /* seal_parallel_replicas */ count(), sum(p.k) FROM t_seal_probe AS p JOIN t_seal_build AS b ON p.k = b.k
    SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, parallel_replicas_for_non_replicated_merge_tree = 1,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

-- ... a join sharded by primary key ranges ...
SELECT /* seal_join_by_shards */ count(), sum(p.k) FROM t_seal_probe AS p JOIN t_seal_build AS b ON p.k = b.k
    SETTINGS query_plan_join_shard_by_pk_ranges = 1;

-- ... and FINAL.
DROP TABLE IF EXISTS t_seal_probe_final;
CREATE TABLE t_seal_probe_final (k UInt64, v String) ENGINE = ReplacingMergeTree ORDER BY k
    SETTINGS index_granularity = 8;
INSERT INTO t_seal_probe_final SELECT number, toString(number) FROM numbers(20000);
INSERT INTO t_seal_probe_final SELECT number, toString(number) FROM numbers(20000);
SELECT /* seal_final */ count(), sum(p.k) FROM t_seal_probe_final AS p FINAL JOIN t_seal_build AS b ON p.k = b.k;
DROP TABLE t_seal_probe_final;

-- A reverse-sorted primary key must be analyzed with its sort direction: the seal-gated
-- pruning has to keep exactly the marks containing the build keys.
DROP TABLE IF EXISTS t_seal_probe_desc;
CREATE TABLE t_seal_probe_desc (k UInt64, v String) ENGINE = MergeTree ORDER BY (k DESC)
    SETTINGS index_granularity = 8;
INSERT INTO t_seal_probe_desc SELECT number, toString(number) FROM numbers(50000);
OPTIMIZE TABLE t_seal_probe_desc FINAL;
SELECT /* seal_gated_reverse_key */ count(), sum(p.k) FROM t_seal_probe_desc AS p JOIN t_seal_build AS b ON p.k = b.k;
DROP TABLE t_seal_probe_desc;

-- A gated read skips the redundant read-time index analysis by the same runtime filter:
-- the marks are dropped by the refiner at task-cut time and no granules reach the
-- read-time pruning.
SELECT /* seal_gated_suppresses_read_time */ count(), sum(p.k) FROM t_seal_probe AS p JOIN t_seal_build AS b ON p.k = b.k
    SETTINGS enable_join_runtime_filters_index_analysis = 1, use_skip_indexes_on_data_read = 1;

-- An ANTI join builds a NOT-contains filter which can never prune positively: the probe
-- side must not be gated (gating would only delay it), and the result stays correct.
SELECT count() FROM t_seal_probe AS p LEFT ANTI JOIN t_seal_build AS b ON p.k = b.k;
SELECT count() > 0 AS has_gated_reads_anti FROM (
    EXPLAIN PIPELINE SELECT count() FROM t_seal_probe AS p LEFT ANTI JOIN t_seal_build AS b ON p.k = b.k
) WHERE explain LIKE '%SealGatedRead%';

-- A String key tracks no [min, max] envelope, so the exact set may be lost to a bloom
-- filter overflow; without a statistics hint that the build side fits, the probe side
-- must not be gated.
DROP TABLE IF EXISTS t_seal_probe_str;
DROP TABLE IF EXISTS t_seal_build_str;
CREATE TABLE t_seal_probe_str (s String) ENGINE = MergeTree ORDER BY s;
INSERT INTO t_seal_probe_str SELECT toString(number) FROM numbers(10000);
CREATE TABLE t_seal_build_str (s String) ENGINE = MergeTree ORDER BY s;
INSERT INTO t_seal_build_str SELECT toString(number * 1000) FROM numbers(5);
SELECT count() FROM t_seal_probe_str AS p JOIN t_seal_build_str AS b ON p.s = b.s;
SELECT count() > 0 AS has_gated_reads_string_key FROM (
    EXPLAIN PIPELINE SELECT count() FROM t_seal_probe_str AS p JOIN t_seal_build_str AS b ON p.s = b.s
) WHERE explain LIKE '%SealGatedRead%';
DROP TABLE t_seal_probe_str;
DROP TABLE t_seal_build_str;

-- Joins whose probe side cannot be gated fall back to ungated reading (fail-open).
SELECT count(), sum(p.k) FROM t_seal_probe AS p LEFT JOIN t_seal_build AS b ON p.k = b.k WHERE b.k = 0;
SELECT count() FROM t_seal_probe AS p RIGHT JOIN t_seal_build AS b ON p.k = b.k;
SELECT count() FROM t_seal_probe AS p JOIN t_seal_build AS b ON cityHash64(p.k) = cityHash64(b.k);

SYSTEM FLUSH LOGS query_log;

-- The probe part has ~6250 marks and the 5 join keys live in at most 10 of them, so almost
-- all marks must be dropped at task-cut time and only a few granules may be read.
-- Do not assert on ReadPoolRangeRefinerDroppedCuts: whether a cut is dropped as a whole
-- depends on the task sizing regime, which is environment-dependent and randomized in CI.
SELECT
    ProfileEvents['ReadPoolRangeRefinerDroppedMarks'] > 6000 AS dropped_marks,
    read_rows < 20000 AS read_few_rows
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query LIKE '%seal_gated_join%'
    AND query NOT LIKE '%query_log%';

-- Without gating nothing is dropped by the refiner and the whole table is read.
SELECT
    ProfileEvents['ReadPoolRangeRefinerDroppedMarks'] AS dropped_marks,
    read_rows >= 50000 AS read_all_rows
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query LIKE '%seal_ungated_join%'
    AND query NOT LIKE '%query_log%';

-- The empty build side prunes every mark of the probe side.
SELECT
    read_rows < 20000 AS read_few_rows
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query LIKE '%seal_empty_build%'
    AND query NOT LIKE '%query_log%';

-- The single-stream read prunes just like the multi-threaded one.
SELECT
    ProfileEvents['ReadPoolRangeRefinerDroppedMarks'] > 6000 AS dropped_marks,
    read_rows < 20000 AS read_few_rows
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query LIKE '%seal_gated_single_stream%'
    AND query NOT LIKE '%query_log%';

-- Reading in order terminates early because of the LIMIT, so only assert that the marks
-- before the matching ones were cut and dropped.
SELECT
    ProfileEvents['ReadPoolRangeRefinerDroppedMarks'] > 0 AS dropped_marks
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query LIKE '%seal_gated_in_order%'
    AND query NOT LIKE '%query_log%';

-- Same for the reverse order.
SELECT
    ProfileEvents['ReadPoolRangeRefinerDroppedMarks'] > 0 AS dropped_marks
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query LIKE '%seal_gated_reverse_order%'
    AND query NOT LIKE '%query_log%';

-- The prefetched pool drops the same marks as the default pool.
SELECT
    ProfileEvents['ReadPoolRangeRefinerDroppedMarks'] > 6000 AS dropped_marks,
    read_rows < 20000 AS read_few_rows
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query LIKE '%seal_gated_prefetched%'
    AND query NOT LIKE '%query_log%';

-- The reverse-sorted key prunes just like the ascending one (and the matching rows,
-- checked above, prove the surviving marks are the right ones).
SELECT
    ProfileEvents['ReadPoolRangeRefinerDroppedMarks'] > 6000 AS dropped_marks,
    read_rows < 20000 AS read_few_rows
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query LIKE '%seal_gated_reverse_key%'
    AND query NOT LIKE '%query_log%';

-- The gated read pruned at task-cut time and the read-time pruning by the same filter was
-- skipped as redundant: no granules were even considered by it.
SELECT
    ProfileEvents['ReadPoolRangeRefinerDroppedMarks'] > 6000 AS dropped_marks,
    ProfileEvents['RuntimeFilterGranulesConsidered'] AS read_time_granules_considered
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query LIKE '%seal_gated_suppresses_read_time%'
    AND query NOT LIKE '%query_log%';

DROP TABLE t_seal_probe;
DROP TABLE t_seal_build;
