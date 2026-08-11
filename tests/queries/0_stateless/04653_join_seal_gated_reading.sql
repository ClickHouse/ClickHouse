-- Tags: long
-- long: the test itself takes about a second, but it covers many scenarios (each with its
-- own DDL and a fill), and the flaky check runs several instances of it in parallel against
-- one debug server; the tag lifts the flaky-check run-time cap which such runners exceed.

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

DROP TABLE IF EXISTS t_seal_probe;
DROP TABLE IF EXISTS t_seal_build;

CREATE TABLE t_seal_probe (k UInt64, v String) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 8;
INSERT INTO t_seal_probe SELECT number, toString(number) FROM numbers(50000);

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
-- parallel replicas (the seal cannot cross replicas; no query-log marker, the read counters
-- of the initiator depend on the work distribution between the replicas)...
SELECT count(), sum(p.k) FROM t_seal_probe AS p JOIN t_seal_build AS b ON p.k = b.k
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
-- filter overflow (on the value count or on the byte size): the probe side must not
-- be gated.
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

-- With a composite primary key, gating requires the filters to cover a key PREFIX: a filter
-- on the second key column alone selects rows scattered over the whole part and cannot cut
-- ranges, so such a probe is not gated (and stays correct).
DROP TABLE IF EXISTS t_seal_probe_ab;
DROP TABLE IF EXISTS t_seal_build_ab;
CREATE TABLE t_seal_probe_ab (a UInt64, b UInt64, v String) ENGINE = MergeTree ORDER BY (a, b)
    SETTINGS index_granularity = 8;
INSERT INTO t_seal_probe_ab SELECT number % 100, intDiv(number, 100), toString(number) FROM numbers(50000);
CREATE TABLE t_seal_build_ab (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY (a, b);
INSERT INTO t_seal_build_ab SELECT number * 20, number * 100 FROM numbers(5);
SELECT count() FROM t_seal_probe_ab AS p JOIN t_seal_build_ab AS q ON p.b = q.b;
SELECT count() > 0 AS has_gated_reads_non_prefix_key FROM (
    EXPLAIN PIPELINE SELECT count() FROM t_seal_probe_ab AS p JOIN t_seal_build_ab AS q ON p.b = q.b
) WHERE explain LIKE '%SealGatedRead%';

-- Both key columns joined (written in the reverse order) cover the (a, b) prefix: the read
-- is gated and the refiner prunes by the whole prefix.
SELECT /* seal_gated_pk_prefix */ count() FROM t_seal_probe_ab AS p JOIN t_seal_build_ab AS q ON p.b = q.b AND p.a = q.a;
SELECT count() > 0 AS has_gated_reads_pk_prefix FROM (
    EXPLAIN PIPELINE SELECT count() FROM t_seal_probe_ab AS p JOIN t_seal_build_ab AS q ON p.b = q.b AND p.a = q.a
) WHERE explain LIKE '%SealGatedRead%';
DROP TABLE t_seal_probe_ab;
DROP TABLE t_seal_build_ab;

-- Joins whose probe side cannot be gated fall back to ungated reading (fail-open).
SELECT count(), sum(p.k) FROM t_seal_probe AS p LEFT JOIN t_seal_build AS b ON p.k = b.k WHERE b.k = 0;
SELECT count() FROM t_seal_probe AS p RIGHT JOIN t_seal_build AS b ON p.k = b.k;
SELECT count() FROM t_seal_probe AS p JOIN t_seal_build AS b ON cityHash64(p.k) = cityHash64(b.k);

SYSTEM FLUSH LOGS query_log;

-- One pass over the query log for all the marked queries. Per marker:
--   dropped_most: the refiner dropped almost all of the ~6250 marks at task-cut time (the 5
--                 join keys live in at most a few granules); expected for every gated query
--                 including the LIMIT ones (with only a few surviving granules the in-order
--                 pool drains and drops the rest of its queue), 0 for the ungated/fail-open
--                 shapes and for the empty build side (an empty inner hash table
--                 short-circuits the probe before anything is cut). A join sharded by
--                 primary key ranges keeps its plain-hash-join plan for this tiny build side,
--                 so its row matches the plainly gated one.
--   few_rows/all_rows: the read volume contrast (gated reads touch a few granules; the
--                 ungated shape reads the whole probe table; FINAL reads its own 40k-row
--                 table, so both flags are 0 there).
--   read_time_considered: granules examined by the read-time index analysis; stays 0 for
--                 seal_gated_suppresses_read_time because the gated read skips it as
--                 redundant (and for the rest because the analysis is disabled).
-- Do not assert on ReadPoolRangeRefinerDroppedCuts: whether a cut is dropped as a whole
-- depends on the task sizing regime, which is environment-dependent and randomized in CI.
SELECT
    extract(query, '/\\* (seal_[a-z_]+) \\*/') AS marker,
    ProfileEvents['ReadPoolRangeRefinerDroppedMarks'] > 6000 AS dropped_most,
    read_rows < 20000 AS few_rows,
    read_rows >= 50000 AS all_rows,
    ProfileEvents['RuntimeFilterGranulesConsidered'] AS read_time_considered
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND is_initial_query
    AND query LIKE '%/* seal_%'
    AND query NOT LIKE '%query_log%'
ORDER BY marker;

DROP TABLE t_seal_probe;
DROP TABLE t_seal_build;
