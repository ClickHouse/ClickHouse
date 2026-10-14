--Tests for enable_join_runtime_filters_index_analysis
-- Part 2 of 2: bloom-filter skip index and FixedHashMap runtime filters.
-- The primary-key and set-index workloads live in
-- 04490_join_runtime_filters_index_analysis to keep each test fast enough for slow builds.

SET enable_analyzer = 1;
SET enable_join_runtime_filters = 1;
SET enable_join_runtime_filters_index_analysis = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_join_swap_table = 'false';
-- Left-side join pruning is intentionally disabled under parallel replicas, so pin PR off to
-- exercise the feature (the ParallelReplicas CI job otherwise forces it on).
SET enable_parallel_replicas = 0;

-- Bloom filter in skip index
DROP TABLE IF EXISTS bf_fact;
DROP TABLE IF EXISTS bf_dim;
CREATE TABLE bf_fact (id UInt64, k UInt64, v UInt64, INDEX idx_k k TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 16;
CREATE TABLE bf_dim (k UInt64, tag String) ENGINE = MergeTree ORDER BY k;
INSERT INTO bf_fact SELECT number, number, number FROM numbers(2000);
INSERT INTO bf_dim SELECT number, if(number < 64, 'hot', 'cold') FROM numbers(2000);

DROP TABLE IF EXISTS bf_fact_cap;
DROP TABLE IF EXISTS bf_dim_cap;
CREATE TABLE bf_fact_cap (id UInt64, k UInt64, v UInt64, INDEX idx_k k TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 16;
CREATE TABLE bf_dim_cap (k UInt64, tag String) ENGINE = MergeTree ORDER BY k;
INSERT INTO bf_fact_cap SELECT number, number, number FROM numbers(2000);
INSERT INTO bf_dim_cap SELECT number, if(number < 64, 'hot', 'cold') FROM numbers(2000);

DROP TABLE IF EXISTS jrf_fact;
DROP TABLE IF EXISTS jrf_dim;
CREATE TABLE jrf_fact (id UInt64, k Int32, v UInt64, INDEX idx_k k TYPE set(0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192;
CREATE TABLE jrf_dim (k Int32, tag String) ENGINE = MergeTree ORDER BY k;
INSERT INTO jrf_fact SELECT number, number, number FROM numbers(50100);
-- two dense clusters with a large gap: an exact key set prunes the gap; a [min,max] range does not
INSERT INTO jrf_dim SELECT number, 'a' FROM numbers(100);
INSERT INTO jrf_dim SELECT number + 50000, 'a' FROM numbers(100);

-- Run every profiled workload query up front, then flush the query log once and read
-- all the profile events back in a single pass. Doing one flush + a few scans instead of
-- one flush + one scan per query keeps the test fast under adversarial randomized settings
-- (e.g. fsync_metadata, direct IO, parallel replicas) where a shared query_log is expensive.

SELECT d.tag, sum(f.v)
FROM bf_fact AS f
INNER JOIN bf_dim AS d ON f.k = d.k
WHERE d.tag = 'hot'
GROUP BY d.tag
FORMAT Null
SETTINGS log_comment = '04490_pe_bf';

SELECT d.tag, sum(f.v)
FROM bf_fact_cap AS f
INNER JOIN bf_dim_cap AS d ON f.k = d.k
WHERE d.tag = 'hot'
GROUP BY d.tag
FORMAT Null
SETTINGS log_comment = '04490_pe_bf_cap', join_runtime_filter_exact_values_limit = 100;

-- A skip index disabled by name via `ignore_data_skipping_indices` must stay disabled on the
-- runtime join-pruning path too: `k` is not the primary key here, so with `idx_k` ignored there
-- is nothing left to prune with.
SELECT d.tag, sum(f.v)
FROM bf_fact AS f
INNER JOIN bf_dim AS d ON f.k = d.k
WHERE d.tag = 'hot'
GROUP BY d.tag
FORMAT Null
SETTINGS log_comment = '04490_pe_bf_ignore', ignore_data_skipping_indices = 'idx_k';

-- join_algorithm='hash' + external-join disabled forces the single-level in-memory HashJoin
-- that converts to a FixedHashMap and publishes it as the runtime filter
SELECT d.tag, sum(f.v)
FROM jrf_fact AS f
INNER JOIN jrf_dim AS d ON f.k = d.k
GROUP BY d.tag
FORMAT Null
SETTINGS log_comment = '04490_pe_fht',
    join_algorithm = 'hash',
    max_bytes_before_external_join = 0,
    max_bytes_ratio_before_external_join = 0,
    enable_join_fixed_hash_table_conversion = 1,
    join_runtime_filter_from_fixed_hash_table = 1;

SYSTEM FLUSH LOGS query_log;

-- The bloom-filter skip index must consider granules and drop some.
-- argMax(..., event_time) picks the latest QueryFinish row per log_comment, mirroring the
-- per-query `ORDER BY event_time DESC LIMIT 1` while collapsing all checks into one scan.
SELECT
    argMax(ProfileEvents['RuntimeFilterGranulesConsidered'], event_time) > 0,
    argMax(ProfileEvents['RuntimeFilterGranulesDropped'], event_time) > 0
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment = '04490_pe_bf'
    AND type = 'QueryFinish'
GROUP BY log_comment
ORDER BY log_comment;

-- The exact-values limit caps the runtime filter, so it must not drop any granule.
SELECT ProfileEvents['RuntimeFilterGranulesDropped'] = 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '04490_pe_bf_cap' AND type = 'QueryFinish'
ORDER BY event_time DESC
LIMIT 1;

-- A skip index disabled via `ignore_data_skipping_indices` must not be used by the runtime path,
-- so no granule may be dropped.
SELECT ProfileEvents['RuntimeFilterGranulesDropped'] = 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '04490_pe_bf_ignore' AND type = 'QueryFinish'
ORDER BY event_time DESC
LIMIT 1;

-- exact-set pruning must survive the FixedHashMap runtime filter, not degrade to a [min,max] range
SELECT ProfileEvents['RuntimeFilterGranulesDropped'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '04490_pe_fht' AND type = 'QueryFinish'
ORDER BY event_time DESC
LIMIT 1;

-- Correctness: pruning must never change results. For each risky surface, verify feature off == on
-- (a too-aggressive predicate that dropped matching granules would make these differ).
SELECT
    (SELECT sum(f.v) FROM bf_fact AS f INNER JOIN bf_dim AS d ON f.k = d.k WHERE d.tag = 'hot' SETTINGS enable_join_runtime_filters_index_analysis = 0) =
    (SELECT sum(f.v) FROM bf_fact AS f INNER JOIN bf_dim AS d ON f.k = d.k WHERE d.tag = 'hot' SETTINGS enable_join_runtime_filters_index_analysis = 1);
SELECT
    (SELECT sum(f.v) FROM jrf_fact AS f INNER JOIN jrf_dim AS d ON f.k = d.k SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0, enable_join_fixed_hash_table_conversion = 1, join_runtime_filter_from_fixed_hash_table = 1, enable_join_runtime_filters_index_analysis = 0) =
    (SELECT sum(f.v) FROM jrf_fact AS f INNER JOIN jrf_dim AS d ON f.k = d.k SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0, enable_join_fixed_hash_table_conversion = 1, join_runtime_filter_from_fixed_hash_table = 1, enable_join_runtime_filters_index_analysis = 1);

DROP TABLE bf_fact;
DROP TABLE bf_dim;
DROP TABLE bf_fact_cap;
DROP TABLE bf_dim_cap;
DROP TABLE jrf_fact;
DROP TABLE jrf_dim;
