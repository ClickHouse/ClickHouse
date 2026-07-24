-- Regression test: a join runtime filter must NOT be served from the query condition cache.
--
-- The runtime-filter handle carried by `__applyFilter` has a deterministic, structural name
-- (`_runtime_filter_<structural_hash>`), so the PREWHERE expression on the probe table is
-- byte-identical across executions of the same plan. The actual filter contents, however, depend
-- on the right side of the join, which differs from execution to execution. `__applyFilter` is
-- therefore marked non-deterministic so it is excluded from the query condition cache: otherwise
-- the first execution would cache a per-granule "no match -> skip" result that a later execution
-- with different right-side keys would reuse, silently dropping rows.
--
-- With the fix every execution returns correct results. If `__applyFilter` were deterministic (or
-- the old query-scoped name lookup were restored), the later executions would reuse the stale
-- granule skips from the first and return 0 rows.

SET enable_analyzer = 1;
SET enable_join_runtime_filters = 1;
SET enable_parallel_replicas = 0;
SET join_algorithm = 'hash';
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET query_plan_join_swap_table = 0;
SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;
SET use_query_condition_cache = 1;

DROP TABLE IF EXISTS rf_qcc_probe;
DROP TABLE IF EXISTS rf_qcc_build;

-- Probe (left) table: a small granule (4 rows) over keys 0..39, ordered by the key, so the three
-- probe keys used below (2, 21, 37) fall into three different granules (the 1st, 6th and 10th).
CREATE TABLE rf_qcc_probe (p_key Int32, payload Int32) ENGINE = MergeTree ORDER BY payload
    SETTINGS index_granularity = 4;
INSERT INTO rf_qcc_probe SELECT number, number FROM numbers(40);

-- Build (right) table: `cat` selects which key the runtime filter is built from.
CREATE TABLE rf_qcc_build (b_key Int32, cat UInt8) ENGINE = MergeTree ORDER BY b_key;
INSERT INTO rf_qcc_build VALUES (2, 1) (21, 2) (37, 3);

-- Each query builds the same structurally-named runtime filter, applied as PREWHERE on the probe
-- table, but with a different key (in a different granule). The first query populates the query
-- condition cache; the later ones must not be served the stale granule skips.
SELECT count(), max(payload) FROM rf_qcc_probe, rf_qcc_build WHERE p_key = b_key AND cat = 1;
SELECT count(), max(payload) FROM rf_qcc_probe, rf_qcc_build WHERE p_key = b_key AND cat = 2;
SELECT count(), max(payload) FROM rf_qcc_probe, rf_qcc_build WHERE p_key = b_key AND cat = 3;

DROP TABLE rf_qcc_probe;
DROP TABLE rf_qcc_build;
