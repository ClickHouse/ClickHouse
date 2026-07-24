-- Tags: no-random-settings, no-random-merge-tree-settings
-- no-random-settings, no-random-merge-tree-settings: the ProfileEvent assertions below pin the
-- runtime-filter pruning decision, which random settings can perturb.

-- Coverage for JOIN runtime-filter primary-key pruning on a reverse (DESC) sorting key.
-- The read-time dynamic KeyCondition built in MergeTreeIndexReadResultPool (the
-- enable_join_runtime_filters_index_analysis path) must honor the key's per-column sort
-- directions. If a DESC primary-key column is analyzed as ascending, the pruning inverts the
-- value interval and either drops granules that hold matching rows (a plain range predicate,
-- changing the result) or produces an invalid MergeTreeSetIndex range (an IN-set predicate,
-- which aborts in debug builds and fail-opens in release builds, so no granules are dropped).
-- This path does not exist in older versions, so it is not covered by
-- 04612_reverse_key_index_analysis. See PR #111059.

SET enable_analyzer = 1;
SET enable_join_runtime_filters = 1;
SET enable_join_runtime_filters_index_analysis = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_join_swap_table = 'false';
-- Left-side join pruning is intentionally disabled under parallel replicas, so pin it off to
-- exercise the feature (the ParallelReplicas CI job otherwise forces it on).
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS rk_fact_desc_second;
DROP TABLE IF EXISTS rk_fact_desc_first;
DROP TABLE IF EXISTS rk_fact_desc_single;
DROP TABLE IF EXISTS rk_dim_second;
DROP TABLE IF EXISTS rk_dim_r;

-- DESC as the second key column (the shape from the bug report: ORDER BY (g, r DESC)).
CREATE TABLE rk_fact_desc_second (g UInt32, r Int32, v UInt64)
ENGINE = MergeTree ORDER BY (g, r DESC) SETTINGS index_granularity = 16;
-- DESC as the leading key column.
CREATE TABLE rk_fact_desc_first (r Int32, g UInt32, v UInt64)
ENGINE = MergeTree ORDER BY (r DESC, g) SETTINGS index_granularity = 16;
-- A single reverse key column.
CREATE TABLE rk_fact_desc_single (r Int32, v UInt64)
ENGINE = MergeTree ORDER BY r DESC SETTINGS index_granularity = 16;

-- Every g holds every r in 0..99, so a DESC r column produces a physical layout genuinely
-- different from ASC (r descends 99..0 within a g group).
INSERT INTO rk_fact_desc_second SELECT number % 20, intDiv(number, 20) % 100, number FROM numbers(4000);
INSERT INTO rk_fact_desc_first  SELECT intDiv(number, 20) % 100, number % 20, number FROM numbers(4000);
INSERT INTO rk_fact_desc_single SELECT number % 100, number FROM numbers(4000);

-- The hot subset selects only on r (r >= 90) and covers every g, so the g runtime filter is
-- non-selective and only the reverse r column can prune granules. A pruner that treats the
-- DESC r column as ascending therefore cannot legitimately drop the matching granules: the
-- result must stay equal to the feature-off result, and pruning must still drop granules
-- (in a release build a wrong direction fail-opens and drops nothing, so `dropped` would be 0).
CREATE TABLE rk_dim_second (g UInt32, r Int32, tag String) ENGINE = MergeTree ORDER BY (g, r);
CREATE TABLE rk_dim_r (r Int32, tag String) ENGINE = MergeTree ORDER BY r;
INSERT INTO rk_dim_second SELECT number % 20, intDiv(number, 20) % 100, if(intDiv(number, 20) % 100 >= 90, 'hot', 'cold') FROM numbers(4000);
INSERT INTO rk_dim_r SELECT number, if(number >= 90, 'hot', 'cold') FROM numbers(100);

-- Correctness: pruning must never change results, so feature on must equal feature off.
SELECT 'desc_second: on == off',
    (SELECT sum(f.v) FROM rk_fact_desc_second AS f INNER JOIN rk_dim_second AS d ON f.g = d.g AND f.r = d.r WHERE d.tag = 'hot' SETTINGS enable_join_runtime_filters_index_analysis = 0)
  = (SELECT sum(f.v) FROM rk_fact_desc_second AS f INNER JOIN rk_dim_second AS d ON f.g = d.g AND f.r = d.r WHERE d.tag = 'hot' SETTINGS enable_join_runtime_filters_index_analysis = 1);

SELECT 'desc_first: on == off',
    (SELECT sum(f.v) FROM rk_fact_desc_first AS f INNER JOIN rk_dim_r AS d ON f.r = d.r WHERE d.tag = 'hot' SETTINGS enable_join_runtime_filters_index_analysis = 0)
  = (SELECT sum(f.v) FROM rk_fact_desc_first AS f INNER JOIN rk_dim_r AS d ON f.r = d.r WHERE d.tag = 'hot' SETTINGS enable_join_runtime_filters_index_analysis = 1);

SELECT 'desc_single: on == off',
    (SELECT sum(f.v) FROM rk_fact_desc_single AS f INNER JOIN rk_dim_r AS d ON f.r = d.r WHERE d.tag = 'hot' SETTINGS enable_join_runtime_filters_index_analysis = 0)
  = (SELECT sum(f.v) FROM rk_fact_desc_single AS f INNER JOIN rk_dim_r AS d ON f.r = d.r WHERE d.tag = 'hot' SETTINGS enable_join_runtime_filters_index_analysis = 1);

-- Run the same joins once more so the pruning ProfileEvents are recorded for each shape.
SELECT sum(f.v) FROM rk_fact_desc_second AS f INNER JOIN rk_dim_second AS d ON f.g = d.g AND f.r = d.r WHERE d.tag = 'hot' FORMAT Null SETTINGS log_comment = '04628_desc_second';
SELECT sum(f.v) FROM rk_fact_desc_first  AS f INNER JOIN rk_dim_r AS d ON f.r = d.r WHERE d.tag = 'hot' FORMAT Null SETTINGS log_comment = '04628_desc_first';
SELECT sum(f.v) FROM rk_fact_desc_single AS f INNER JOIN rk_dim_r AS d ON f.r = d.r WHERE d.tag = 'hot' FORMAT Null SETTINGS log_comment = '04628_desc_single';

SYSTEM FLUSH LOGS query_log;

-- The assertions above are only meaningful if pruning actually engaged on each reverse-key
-- shape: it must consider granules and drop at least one. The drop can only come from the
-- reverse r column (the g filter is non-selective), so this also fails if a release build
-- fail-opens on a wrongly-inverted r range.
SELECT log_comment,
    argMax(ProfileEvents['RuntimeFilterGranulesConsidered'], event_time) > 0 AS considered,
    argMax(ProfileEvents['RuntimeFilterGranulesDropped'], event_time) > 0 AS dropped
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment IN ('04628_desc_second', '04628_desc_first', '04628_desc_single')
    AND type = 'QueryFinish'
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE rk_fact_desc_second;
DROP TABLE rk_fact_desc_first;
DROP TABLE rk_fact_desc_single;
DROP TABLE rk_dim_second;
DROP TABLE rk_dim_r;
