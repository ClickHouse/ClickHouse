-- Tags: no-random-settings

-- Bernoulli sampling for MergeTree tables without SAMPLE BY key

DROP TABLE IF EXISTS t_bernoulli;
DROP TABLE IF EXISTS t_bernoulli_empty;
DROP TABLE IF EXISTS t_bernoulli_memory;
DROP TABLE IF EXISTS t_bernoulli_multi;
DROP TABLE IF EXISTS t_bernoulli_large;

CREATE TABLE t_bernoulli (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_bernoulli SELECT number FROM numbers(100000);

SELECT 'error without experimental setting';
SELECT count() FROM t_bernoulli SAMPLE 0.1; -- { serverError SAMPLING_NOT_SUPPORTED }

SET allow_experimental_bernoulli_sample = 1;

SELECT 'basic sample 0.1';
SELECT count() FROM t_bernoulli SAMPLE 0.1 SETTINGS bernoulli_sample_seed = 42;

SELECT 'basic sample 0.5';
SELECT COUNT() FROM t_bernoulli SAMPLE 0.5 SETTINGS bernoulli_sample_seed = 42;

SELECT 'basic sample 0.9';
SELECT COUNT() FROM t_bernoulli SAMPLE 0.9 SETTINGS bernoulli_sample_seed = 42;

SELECT 'sample_factor';
SELECT DISTINCT _sample_factor FROM t_bernoulli SAMPLE 0.1 SETTINGS bernoulli_sample_seed = 42;

SELECT 'offset without sample by key';
SELECT count() FROM t_bernoulli SAMPLE 0.1 OFFSET 0.5; -- { serverError SAMPLING_NOT_SUPPORTED }
SELECT count() FROM t_bernoulli SAMPLE 50000 OFFSET 10000; -- { serverError SAMPLING_NOT_SUPPORTED }

SELECT 'sample 0 (no-op)';
SELECT count() FROM t_bernoulli SAMPLE 0;

SELECT 'sample 1 (no-op)';
SELECT count() FROM t_bernoulli SAMPLE 1;

SELECT 'absolute sample exceeding table size';
SELECT count() FROM t_bernoulli SAMPLE 200000;

SELECT 'absolute sample 50000';
SELECT count() FROM t_bernoulli SAMPLE 50000 SETTINGS bernoulli_sample_seed = 42;

SELECT 'sample_factor absolute';
SELECT DISTINCT _sample_factor FROM t_bernoulli SAMPLE 50000 SETTINGS bernoulli_sample_seed = 42;

SELECT 'sample_factor absolute exceeding table size';
SELECT DISTINCT _sample_factor FROM t_bernoulli SAMPLE 200000;

SELECT 'empty table';
CREATE TABLE t_bernoulli_empty (x UInt64) ENGINE = MergeTree ORDER BY x;
SELECT count() FROM t_bernoulli_empty SAMPLE 0.1 SETTINGS bernoulli_sample_seed = 42;

SELECT 'parallel replicas sampling_key mode without sample by';
-- In the legacy 'sampling_key' mode, there is no key to split across replicas, so only
-- replica 0 performs Bernoulli sampling while non-first replicas read nothing.
-- With the modern 'read_tasks' mode (default), work is split by mark ranges and each
-- replica applies Bernoulli sampling independently — no such limitation.
SELECT count() FROM t_bernoulli SAMPLE 0.1 SETTINGS
    bernoulli_sample_seed = 42,
    enable_parallel_replicas = 1,
    max_parallel_replicas = 2,
    parallel_replicas_mode = 'sampling_key',
    parallel_replicas_count = 2,
    parallel_replica_offset = 0,
    parallel_replicas_for_non_replicated_merge_tree = 1;
SELECT count() FROM t_bernoulli SAMPLE 0.1 SETTINGS
    bernoulli_sample_seed = 42,
    enable_parallel_replicas = 1,
    max_parallel_replicas = 2,
    parallel_replicas_mode = 'sampling_key',
    parallel_replicas_count = 2,
    parallel_replica_offset = 1,
    parallel_replicas_for_non_replicated_merge_tree = 1;

SELECT 'non-MergeTree engine rejected';
CREATE TABLE t_bernoulli_memory (x UInt64) ENGINE = Memory;
INSERT INTO t_bernoulli_memory SELECT number FROM numbers(100);
SELECT count() FROM t_bernoulli_memory SAMPLE 0.1; -- { serverError SAMPLING_NOT_SUPPORTED }

SELECT 'very small probability';
SELECT count() FROM t_bernoulli SAMPLE 0.001 SETTINGS bernoulli_sample_seed = 42;

SELECT 'multi-part table';
DROP TABLE IF EXISTS t_bernoulli_multi;
CREATE TABLE t_bernoulli_multi (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_bernoulli_multi SELECT number FROM numbers(50000);
INSERT INTO t_bernoulli_multi SELECT number + 50000 FROM numbers(50000);
INSERT INTO t_bernoulli_multi SELECT number + 100000 FROM numbers(50000);
SELECT count() FROM t_bernoulli_multi SAMPLE 0.1 SETTINGS bernoulli_sample_seed = 42;

SELECT 'thread-count independence';
SELECT
    (SELECT count() FROM t_bernoulli SAMPLE 0.1 SETTINGS bernoulli_sample_seed = 42, max_threads = 1)
    =
    (SELECT count() FROM t_bernoulli SAMPLE 0.1 SETTINGS bernoulli_sample_seed = 42, max_threads = 4);

SELECT 'multi-part thread-count independence';
SELECT
    (SELECT count() FROM t_bernoulli_multi SAMPLE 0.1 SETTINGS bernoulli_sample_seed = 42, max_threads = 1)
    =
    (SELECT count() FROM t_bernoulli_multi SAMPLE 0.1 SETTINGS bernoulli_sample_seed = 42, max_threads = 4);

SELECT 'large table with granule skipping (p=0.001)';
DROP TABLE IF EXISTS t_bernoulli_large;
CREATE TABLE t_bernoulli_large (x UInt64) ENGINE = MergeTree ORDER BY x;
-- 2M rows = ~244 granules at default 8192 granularity.
-- At p=0.001, expected ~2000 hits. Each granule expects ~8 hits,
-- but some will have zero (P ≈ 0.0002), exercising granule skipping.
INSERT INTO t_bernoulli_large SELECT number FROM numbers(2000000);

SELECT 'large count in range';
SELECT count() FROM t_bernoulli_large SAMPLE 0.001 SETTINGS bernoulli_sample_seed = 42;

SELECT 'large bit-exact determinism (checksum)';
SELECT
    (SELECT sum(cityHash64(x)) FROM t_bernoulli_large SAMPLE 0.001 SETTINGS bernoulli_sample_seed = 42, max_threads = 1)
    =
    (SELECT sum(cityHash64(x)) FROM t_bernoulli_large SAMPLE 0.001 SETTINGS bernoulli_sample_seed = 42, max_threads = 7);

SELECT 'very small probability';
SELECT count() FROM t_bernoulli_large SAMPLE 0.0001 SETTINGS bernoulli_sample_seed = 42;

DROP TABLE t_bernoulli_large;

SELECT 'nonadaptive granularity';
DROP TABLE IF EXISTS t_bernoulli_nonadaptive;
CREATE TABLE t_bernoulli_nonadaptive (x UInt64) ENGINE = MergeTree ORDER BY x
SETTINGS index_granularity = 8192, index_granularity_bytes = 0;
INSERT INTO t_bernoulli_nonadaptive SELECT number FROM numbers(100000);
SELECT
    (SELECT count() FROM t_bernoulli SAMPLE 0.1 SETTINGS bernoulli_sample_seed = 42)
    =
    (SELECT count() FROM t_bernoulli_nonadaptive SAMPLE 0.1 SETTINGS bernoulli_sample_seed = 42);
DROP TABLE t_bernoulli_nonadaptive;

SELECT 'bernoulli with skip index';
DROP TABLE IF EXISTS t_bernoulli_skip;
CREATE TABLE t_bernoulli_skip (x UInt64, y UInt64, INDEX idx_y y TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY x
SETTINGS index_granularity = 8192;
INSERT INTO t_bernoulli_skip SELECT number, number FROM numbers(100000);
-- WHERE y >= 50000 eliminates ~half the granules via the skip index.
-- SAMPLE 0.1 with Bernoulli further reduces by ~10x.
SELECT count() FROM t_bernoulli_skip SAMPLE 0.1 WHERE y >= 50000 SETTINGS bernoulli_sample_seed = 42;
DROP TABLE t_bernoulli_skip;

SELECT 'bernoulli with merge table';
DROP TABLE IF EXISTS t_bernoulli_child;
DROP TABLE IF EXISTS t_bernoulli_merge;
CREATE TABLE t_bernoulli_child (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_bernoulli_child SELECT number FROM numbers(100000);
CREATE TABLE t_bernoulli_merge (x UInt64) ENGINE = Merge(currentDatabase(), '^t_bernoulli_child$');
SELECT count() FROM t_bernoulli_merge SAMPLE 0.1 SETTINGS bernoulli_sample_seed = 42;
DROP TABLE t_bernoulli_merge;
DROP TABLE t_bernoulli_child;

SELECT 'bernoulli through alias to merge tree';
-- A wrapper storage (Alias) proxies capability checks to its target. For an alias to a
-- MergeTree table without a SAMPLE BY key, SAMPLE must keep working under the experimental
-- flag: the alias is transparent and the read goes through the target's MergeTree reading
-- path, where the Bernoulli granule filter is applied. Regression guard for the eligibility
-- gate (isMergeTree must be honoured through the wrapper, not rejected with SAMPLING_NOT_SUPPORTED).
DROP TABLE IF EXISTS t_bernoulli_alias_target;
DROP TABLE IF EXISTS t_bernoulli_alias;
CREATE TABLE t_bernoulli_alias_target (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_bernoulli_alias_target SELECT number FROM numbers(100000);
CREATE TABLE t_bernoulli_alias ENGINE = Alias(currentDatabase(), 't_bernoulli_alias_target');
SELECT count() FROM t_bernoulli_alias SAMPLE 0.1 SETTINGS bernoulli_sample_seed = 42;
DROP TABLE t_bernoulli_alias;
DROP TABLE t_bernoulli_alias_target;

SELECT 'prewhere with bernoulli';
SELECT count() FROM t_bernoulli SAMPLE 0.1 PREWHERE x >= 50000 SETTINGS bernoulli_sample_seed = 42;

SELECT 'sample by key dominates the experimental flag';
-- A table with a SAMPLE BY key must always use the native sampling path,
-- regardless of allow_experimental_bernoulli_sample. The Bernoulli branch in
-- MergeTreeDataSelectExecutor::getSampling is only entered when !hasSamplingKey().
DROP TABLE IF EXISTS t_bernoulli_sbk;
CREATE TABLE t_bernoulli_sbk (x UInt64) ENGINE = MergeTree ORDER BY (cityHash64(x), x) SAMPLE BY cityHash64(x);
INSERT INTO t_bernoulli_sbk SELECT number FROM numbers(100000);
-- SAMPLE OFFSET is rejected by the Bernoulli path but supported natively;
-- a successful query here proves the native path is used.
SELECT count() > 0 FROM t_bernoulli_sbk SAMPLE 0.5 OFFSET 0.5;
-- bernoulli_sample_seed has no effect on the native path.
SELECT
    (SELECT count() FROM t_bernoulli_sbk SAMPLE 0.1)
    =
    (SELECT count() FROM t_bernoulli_sbk SAMPLE 0.1 SETTINGS bernoulli_sample_seed = 42);
-- Toggling allow_experimental_bernoulli_sample does not perturb tables with a SAMPLE BY key.
SELECT
    (SELECT count() FROM t_bernoulli_sbk SAMPLE 0.1 SETTINGS allow_experimental_bernoulli_sample = 0)
    =
    (SELECT count() FROM t_bernoulli_sbk SAMPLE 0.1 SETTINGS allow_experimental_bernoulli_sample = 1);

SELECT 'parallel replicas read_tasks mode with bernoulli';
-- In the modern 'read_tasks' mode (the default), every replica builds its own Bernoulli
-- filter over its assigned mark ranges, so all replicas contribute and the total is
-- close to p*N. Unlike the legacy 'sampling_key' mode, no replica reads zero rows.
SELECT count() BETWEEN 8000 AND 12000 FROM t_bernoulli SAMPLE 0.1 SETTINGS
    bernoulli_sample_seed = 42,
    enable_parallel_replicas = 1,
    max_parallel_replicas = 3,
    parallel_replicas_mode = 'read_tasks',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

SELECT 'projection disabled by bernoulli';
-- canUseProjectionForReadingStep in projectionsCommon.cpp short-circuits on
-- isQueryWithSampling(), which is true for any SAMPLE clause (Bernoulli or native).
-- If projections were used under Bernoulli, sum(v) would always equal 100000
-- (the pre-aggregated value) instead of being filtered by the row-level filter.
DROP TABLE IF EXISTS t_bernoulli_proj;
CREATE TABLE t_bernoulli_proj
(
    x UInt64,
    v UInt64,
    PROJECTION p_agg (SELECT sum(v))
)
ENGINE = MergeTree ORDER BY x;
INSERT INTO t_bernoulli_proj SELECT number, 1 FROM numbers(100000);
-- Sanity: without sampling, the aggregating projection IS picked.
SELECT count() > 0 FROM (EXPLAIN SELECT sum(v) FROM t_bernoulli_proj) WHERE explain LIKE '%p_agg%';
-- Negative: with Bernoulli sampling, the projection MUST NOT be used.
SELECT count() FROM (EXPLAIN SELECT sum(v) FROM t_bernoulli_proj SAMPLE 0.1) WHERE explain LIKE '%p_agg%';
-- Value: matches the 9913 fingerprint from the basic Bernoulli test (v=1 so sum == count).
-- A pre-aggregated projection result would have returned 100000 here.
SELECT sum(v) FROM t_bernoulli_proj SAMPLE 0.1 SETTINGS bernoulli_sample_seed = 42;

DROP TABLE t_bernoulli;
DROP TABLE t_bernoulli_empty;
DROP TABLE t_bernoulli_memory;
DROP TABLE t_bernoulli_multi;
DROP TABLE t_bernoulli_sbk;
DROP TABLE t_bernoulli_proj;
