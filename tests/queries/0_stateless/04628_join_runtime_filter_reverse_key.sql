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
-- Both predicate shapes are exercised: the IN-set (MergeTreeSetIndex) path when the build side
-- stays under join_runtime_filter_exact_values_limit, and the [min, max] range path
-- (buildRuntimeRangePredicate) when the exact-values set overflows that limit and is released.
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

-- The hot subset selects two r values that are far apart (10 and 90) and covers every g, so the
-- g runtime filter is non-selective and only the reverse r column can prune granules. Two
-- non-adjacent hot values make the two predicate shapes prune differently, which the range-branch
-- assertion below relies on: the exact IN-set {10, 90} drops every granule that holds neither
-- value, while the [10, 90] range keeps the whole span between them. A pruner that treats the DESC
-- r column as ascending cannot legitimately drop the matching granules, so on == off must hold and
-- pruning must still drop granules (a wrong direction fail-opens in release and drops nothing).
CREATE TABLE rk_dim_second (g UInt32, r Int32, tag String) ENGINE = MergeTree ORDER BY (g, r);
CREATE TABLE rk_dim_r (r Int32, tag String) ENGINE = MergeTree ORDER BY r;
INSERT INTO rk_dim_second SELECT number % 20, intDiv(number, 20) % 100, if(intDiv(number, 20) % 100 IN (10, 90), 'hot', 'cold') FROM numbers(4000);
INSERT INTO rk_dim_r SELECT number, if(number IN (10, 90), 'hot', 'cold') FROM numbers(100);

-- IN-set path. Each build side has only two distinct r values, well under the default
-- join_runtime_filter_exact_values_limit, so the read-time KeyCondition is the exact IN-set.
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

-- Range path. Lowering the limit below the two distinct hot values makes the exact-values set
-- overflow and be released, so the read-time KeyCondition is built from the recorded [10, 90]
-- range envelope instead. Correctness must still hold on the reverse key.
SELECT 'desc_second range: on == off',
    (SELECT sum(f.v) FROM rk_fact_desc_second AS f INNER JOIN rk_dim_second AS d ON f.g = d.g AND f.r = d.r WHERE d.tag = 'hot' SETTINGS enable_join_runtime_filters_index_analysis = 0)
  = (SELECT sum(f.v) FROM rk_fact_desc_second AS f INNER JOIN rk_dim_second AS d ON f.g = d.g AND f.r = d.r WHERE d.tag = 'hot' SETTINGS enable_join_runtime_filters_index_analysis = 1, join_runtime_filter_exact_values_limit = 1);

SELECT 'desc_first range: on == off',
    (SELECT sum(f.v) FROM rk_fact_desc_first AS f INNER JOIN rk_dim_r AS d ON f.r = d.r WHERE d.tag = 'hot' SETTINGS enable_join_runtime_filters_index_analysis = 0)
  = (SELECT sum(f.v) FROM rk_fact_desc_first AS f INNER JOIN rk_dim_r AS d ON f.r = d.r WHERE d.tag = 'hot' SETTINGS enable_join_runtime_filters_index_analysis = 1, join_runtime_filter_exact_values_limit = 1);

SELECT 'desc_single range: on == off',
    (SELECT sum(f.v) FROM rk_fact_desc_single AS f INNER JOIN rk_dim_r AS d ON f.r = d.r WHERE d.tag = 'hot' SETTINGS enable_join_runtime_filters_index_analysis = 0)
  = (SELECT sum(f.v) FROM rk_fact_desc_single AS f INNER JOIN rk_dim_r AS d ON f.r = d.r WHERE d.tag = 'hot' SETTINGS enable_join_runtime_filters_index_analysis = 1, join_runtime_filter_exact_values_limit = 1);

-- Record the pruning ProfileEvents for each shape on both paths.
SELECT sum(f.v) FROM rk_fact_desc_second AS f INNER JOIN rk_dim_second AS d ON f.g = d.g AND f.r = d.r WHERE d.tag = 'hot' FORMAT Null SETTINGS log_comment = '04628_in_desc_second';
SELECT sum(f.v) FROM rk_fact_desc_first  AS f INNER JOIN rk_dim_r AS d ON f.r = d.r WHERE d.tag = 'hot' FORMAT Null SETTINGS log_comment = '04628_in_desc_first';
SELECT sum(f.v) FROM rk_fact_desc_single AS f INNER JOIN rk_dim_r AS d ON f.r = d.r WHERE d.tag = 'hot' FORMAT Null SETTINGS log_comment = '04628_in_desc_single';
SELECT sum(f.v) FROM rk_fact_desc_second AS f INNER JOIN rk_dim_second AS d ON f.g = d.g AND f.r = d.r WHERE d.tag = 'hot' FORMAT Null SETTINGS join_runtime_filter_exact_values_limit = 1, log_comment = '04628_range_desc_second';
SELECT sum(f.v) FROM rk_fact_desc_first  AS f INNER JOIN rk_dim_r AS d ON f.r = d.r WHERE d.tag = 'hot' FORMAT Null SETTINGS join_runtime_filter_exact_values_limit = 1, log_comment = '04628_range_desc_first';
SELECT sum(f.v) FROM rk_fact_desc_single AS f INNER JOIN rk_dim_r AS d ON f.r = d.r WHERE d.tag = 'hot' FORMAT Null SETTINGS join_runtime_filter_exact_values_limit = 1, log_comment = '04628_range_desc_single';

SYSTEM FLUSH LOGS query_log;

-- Per shape, prove both predicate shapes engaged and that the range branch really ran rather than
-- silently falling back to the IN-set. Because the two hot values are far apart, the exact IN-set
-- prunes every granule outside {10, 90} while the [10, 90] range keeps the whole span, so the range
-- path must drop strictly fewer granules than the IN-set path and still drop at least one. A silent
-- IN-set fallback would make the two counts equal; a wrongly-inverted reverse-key range would drop
-- nothing.
SELECT shape,
    maxIf(dropped, is_in) > 0 AS in_prunes,
    maxIf(dropped, NOT is_in) > 0 AS range_prunes,
    maxIf(dropped, NOT is_in) < maxIf(dropped, is_in) AS range_drops_fewer
FROM
(
    SELECT
        replaceRegexpOne(log_comment, '^04628_(in|range)_', '') AS shape,
        log_comment LIKE '04628\_in\_%' AS is_in,
        argMax(ProfileEvents['RuntimeFilterGranulesDropped'], event_time) AS dropped
    FROM system.query_log
    WHERE current_database = currentDatabase()
        AND log_comment IN ('04628_in_desc_second', '04628_in_desc_first', '04628_in_desc_single',
                            '04628_range_desc_second', '04628_range_desc_first', '04628_range_desc_single')
        AND type = 'QueryFinish'
    GROUP BY log_comment
)
GROUP BY shape
ORDER BY shape;

DROP TABLE rk_fact_desc_second;
DROP TABLE rk_fact_desc_first;
DROP TABLE rk_fact_desc_single;
DROP TABLE rk_dim_second;
DROP TABLE rk_dim_r;
