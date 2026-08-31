-- Tags: no-parallel, no-parallel-replicas
-- no-parallel: drops the (instance-wide) query condition cache
-- no-parallel-replicas: the query condition cache is populated per replica, so the mark
--                       accounting below is deterministic only on a single replica

-- Issue #113098: a query served from a materialized projection consulted the query condition cache
-- but never populated it, so every repetition re-read the same marks. The projection read path
-- built its cache part name differently on the write and the read side, and the plan-side pass that
-- tags a `FilterStep` with the cache key did not run again after a projection was applied.

-- Fixture, shared by every section below. Everything shape-specific (block size, thread count,
-- aggregation and TopK knobs) is set per query instead of globally, so the aggregate, normal-
-- projection and TopK sections can each pin what they need without disturbing the others.
SET use_query_condition_cache = 1;
-- The cache needs the analyzer on both the write and the read side.
SET enable_analyzer = 1;
SET optimize_use_projections = 1;
-- The explicit projection below is what this test is about; keep the implicit
-- `_minmax_count_projection` and projection-based part filtering out of the picture.
SET optimize_use_implicit_projections = 0, optimize_use_projection_filtering = 0;
-- Randomized statistics would prune the part outright, leaving nothing to read and the mark
-- accounting below vacuous.
SET use_statistics_for_part_pruning = 0;

DROP TABLE IF EXISTS t_qcc_proj;

-- Both projections are declared here rather than added and materialized afterwards, so the `INSERT`
-- below builds their parts and no mutation is involved. What the test needs is projection data in
-- the part, which either route produces; the difference is the cost. `MATERIALIZE PROJECTION` waits
-- for a background mutation that shares its pool with everything else the server has queued, and in
-- the flaky check - where this test is the only one running, repeatedly, on a server still merging
-- the `test.hits` dataset and its own growing `system.*_log` tables - each of the two mutations took
-- ~70s of the ~196s run, in the release build as much as in the debug one. Neither the read path
-- under test nor the shape of the parts depends on how the projection got its data.
--
-- `a` is only here to make `b` a non-leading key of each projection's sort order, which is the
-- realistic shape: a projection built for one access pattern, filtered on a column that is not its
-- leading key. The projection's own primary key then cannot prune `b = 19999`, so what is left over
-- is exactly what the condition cache is supposed to eliminate.
CREATE TABLE t_qcc_proj (pk UInt64, a UInt32, b UInt32,
    PROJECTION p (SELECT a, b, count() GROUP BY a, b),
    PROJECTION pn (SELECT pk, a, b ORDER BY a))
ENGINE = MergeTree ORDER BY pk
SETTINGS index_granularity = 64, add_minmax_index_for_numeric_columns = 0, auto_statistics_types = '';

-- `b` holds only even values, so the odd needle used below sits inside [min, max] but matches no
-- row: neither the base table's primary key nor the projection's own key can prune it away.
INSERT INTO t_qcc_proj SELECT number, number % 1000, number * 2 FROM numbers(20000);

-- With a `count()` the filter is not moved into PREWHERE, so the cache is written from the
-- `FilterTransform` above the read, which can only record a chunk that was filtered out *entirely*.
-- `max_threads` and `max_block_size` therefore decide how much of a part is recordable and are
-- pinned on every query below: one thread, and a block size well below the part size (the
-- matching-needle section relies on the part spanning many chunks).

SELECT '--- baseline: no projection, prime reads everything, reuse prunes';

SYSTEM DROP QUERY CONDITION CACHE;

-- `optimize_use_projections = 0` is what makes this section the baseline: the projections exist from
-- the start, and `p` would otherwise serve this `count()` exactly as in the next section.
SELECT count() FROM t_qcc_proj WHERE b = 19999
SETTINGS max_threads = 1, max_block_size = 8192, optimize_aggregation_in_order = 0,
    optimize_use_projections = 0, log_comment = '05045_assert_base_prime';
SELECT count() FROM t_qcc_proj WHERE b = 19999
SETTINGS max_threads = 1, max_block_size = 8192, optimize_aggregation_in_order = 0,
    optimize_use_projections = 0, log_comment = '05045_assert_base_reuse';


SELECT '--- served from the materialized projection, prime reads everything, reuse prunes';

SYSTEM DROP QUERY CONDITION CACHE;

-- `force_optimize_projection_name` makes the test fail loudly if the read ever stops being served
-- by the projection, instead of silently degrading into a copy of the baseline above.
SELECT count() FROM t_qcc_proj WHERE b = 19999
SETTINGS max_threads = 1, max_block_size = 8192, optimize_aggregation_in_order = 0,
    force_optimize_projection_name = 'p', log_comment = '05045_assert_proj_prime';
SELECT count() FROM t_qcc_proj WHERE b = 19999
SETTINGS max_threads = 1, max_block_size = 8192, optimize_aggregation_in_order = 0,
    force_optimize_projection_name = 'p', log_comment = '05045_assert_proj_reuse';


SELECT '--- a needle that does match: fewer marks on reuse, but not zero';

SYSTEM DROP QUERY CONDITION CACHE;

SELECT count() FROM t_qcc_proj WHERE b = 19998
SETTINGS max_threads = 1, max_block_size = 8192, optimize_aggregation_in_order = 0,
    force_optimize_projection_name = 'p', log_comment = '05045_assert_proj_match_prime';
SELECT count() FROM t_qcc_proj WHERE b = 19998
SETTINGS max_threads = 1, max_block_size = 8192, optimize_aggregation_in_order = 0,
    force_optimize_projection_name = 'p', log_comment = '05045_assert_proj_match_reuse';


-- The non-aggregating counterpart, on the same fixture: `pn` is sorted by `a`, so - just like `p` -
-- its own primary key cannot prune the `b` predicate and every projection granule has to be read.
-- Unlike the `count()` shape above, this filter does reach PREWHERE, so the cache is written from
-- the reader with per-mark granularity. `p` is on the table throughout: only `Type::Normal`
-- projections are candidates here, so the aggregate one is not competing, and its presence pins that.

SELECT '--- served from a materialized normal projection, prime reads everything, reuse prunes';

SYSTEM DROP QUERY CONDITION CACHE;

-- On the prime run `pn` reads exactly as many marks as the base table - neither can prune `b` - and
-- an equal-cost projection is taken only when `force_optimize_projection` is on (the
-- `sum_marks == parent_reading_marks` branch of `optimizeUseNormalProjection.cpp`);
-- `force_optimize_projection_name` pins which one and makes the test fail loudly if the read is
-- served from the base table instead. On the reuse run the projection wins on cost alone: the cache
-- prunes its parts to zero marks during candidate analysis. That consult is the path that used to
-- probe the bare projection part name while the write side keyed on `<parent_part>:<projection>`,
-- so it could never hit.
SELECT pk FROM t_qcc_proj WHERE b = 19999 FORMAT Null
SETTINGS max_threads = 1, max_block_size = 8192, force_optimize_projection = 1,
    force_optimize_projection_name = 'pn', log_comment = '05045_assert_norm_prime';
SELECT pk FROM t_qcc_proj WHERE b = 19999 FORMAT Null
SETTINGS max_threads = 1, max_block_size = 8192, force_optimize_projection = 1,
    force_optimize_projection_name = 'pn', log_comment = '05045_assert_norm_reuse';


SELECT '--- the normal projection still returns the planted row with the cache warm';
SELECT pk FROM t_qcc_proj WHERE b = 19998
SETTINGS max_threads = 1, force_optimize_projection = 1, force_optimize_projection_name = 'pn';

SELECT '--- a TopK read served by the projection primes and reuses the cache';

SYSTEM DROP QUERY CONDITION CACHE;

-- `tryOptimizeTopK` stamps the base read, then `optimizeUseNormalProjections` replaces it and
-- carries the stamp over (`copyTopKFilterInfoAndQueryConditionCacheGate`). Three settings keep the
-- stamp alive, and each one silently removes it if wrong:
--   * `ORDER BY a`, not the table's own sort key - sorting by `pk` activates read-in-order, the
--     `SortingStep` stops being `Full`, and `tryOptimizeTopK` bails (see
--     `04051_top_k_dynamic_filter_read_in_order`).
--   * `use_top_k_dynamic_filtering = 0` - the skip-index shape, as in
--     `04658_query_condition_cache_topk_normal_projection`. With dynamic filtering on, the
--     `__topKFilter` node reaches the projection's PREWHERE, where `MergeTreeSelectProcessor`'s
--     determinism check rejects the write and nothing is cached at all.
--   * `force_optimize_projection` - `pn` costs the same as the base table here, and equal-cost
--     projections are declined without it.
SELECT pk FROM t_qcc_proj WHERE b = 19999 ORDER BY a LIMIT 5 FORMAT Null
SETTINGS max_threads = 1, max_block_size = 8192, optimize_move_to_prewhere = 0,
    use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 1,
    query_plan_max_limit_for_top_k_optimization = 1000,
    force_optimize_projection = 1, force_optimize_projection_name = 'pn',
    log_comment = '05045_assert_topk_idx_prime';
SELECT pk FROM t_qcc_proj WHERE b = 19999 ORDER BY a LIMIT 5 FORMAT Null
SETTINGS max_threads = 1, max_block_size = 8192, optimize_move_to_prewhere = 0,
    use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 1,
    query_plan_max_limit_for_top_k_optimization = 1000,
    force_optimize_projection = 1, force_optimize_projection_name = 'pn',
    log_comment = '05045_assert_topk_idx_reuse';


SELECT '--- correctness: a plain read must not lose rows to entries a TopK read left behind';

SYSTEM DROP QUERY CONDITION CACHE;

-- The sections above all use a needle that matches nothing, so the `LIMIT` never fills, the pipeline
-- is never cancelled early, and every granule the reader records was read in full. That is exactly
-- the case which cannot tell a sound entry from an unsound one. `b < 1000` matches 500 rows, so the
-- `LIMIT 5` fills almost immediately and the read stops mid-part. If `addPrewhereUnmatchedMarks` can
-- record a granule the TopK threshold left partially read, the plain reads below lose rows.
SELECT pk FROM t_qcc_proj WHERE b < 1000 ORDER BY a LIMIT 5 FORMAT Null
SETTINGS max_threads = 1, max_block_size = 8192, optimize_move_to_prewhere = 0,
    use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 1,
    query_plan_max_limit_for_top_k_optimization = 1000,
    force_optimize_projection = 1, force_optimize_projection_name = 'pn',
    log_comment = '05045_assert_poison_topk';

-- Both must return 500: through the projection, whose parts carry the entries just written, and
-- through the base table as a control. The `LIMIT` (far above the 500 matching rows, so it changes
-- no result) keeps the aggregate projection out: `findReadingStep` walks only `Expression` and
-- `Filter` steps, so a `LimitStep` between the aggregation and the read stops `p` from serving the
-- `count()` - which is what it would otherwise do, exactly as in the sections above.
SELECT count() FROM (SELECT pk FROM t_qcc_proj WHERE b < 1000 LIMIT 1000000)
SETTINGS max_threads = 1, force_optimize_projection = 1, force_optimize_projection_name = 'pn',
    log_comment = '05045_assert_poison_check_projection';
SELECT count() FROM (SELECT pk FROM t_qcc_proj WHERE b < 1000 LIMIT 1000000)
SETTINGS max_threads = 1, optimize_use_projections = 0,
    log_comment = '05045_assert_poison_check_base';

SELECT '--- an aliased condition shares its cache entry with the unaliased spelling';

-- `Node::updateHash` folds `result_name` into the hash, so without alias resolution `cond` and the
-- predicate it renames are two different keys and neither run can reuse the other's entry. The
-- projection paths hit this through the synthesized `_projection_filter` alias; this section covers
-- the user-visible spelling, on the base table (`optimize_use_projections = 0`).
SYSTEM DROP QUERY CONDITION CACHE;

SELECT count() FROM t_qcc_proj WHERE b = 19999
SETTINGS max_threads = 1, max_block_size = 8192, optimize_use_projections = 0,
    log_comment = '05045_assert_alias_plain_prime';
SELECT count() FROM t_qcc_proj WHERE (b = 19999) AS cond
SETTINGS max_threads = 1, max_block_size = 8192, optimize_use_projections = 0,
    log_comment = '05045_assert_alias_aliased_reuse';

SYSTEM DROP QUERY CONDITION CACHE;

-- ... and the same in the other direction.
SELECT count() FROM t_qcc_proj WHERE (b = 19999) AS cond
SETTINGS max_threads = 1, max_block_size = 8192, optimize_use_projections = 0,
    log_comment = '05045_assert_alias_aliased_prime';
SELECT count() FROM t_qcc_proj WHERE b = 19999
SETTINGS max_threads = 1, max_block_size = 8192, optimize_use_projections = 0,
    log_comment = '05045_assert_alias_plain_reuse';


-- All assertions are collected here, behind a single `SYSTEM FLUSH LOGS`. Reading `system.query_log`
-- costs the whole time window, not just this test's rows - and `ProfileEvents` is a `Map` column - so
-- a scan per section makes the test's runtime scale with how busy the server has been rather than
-- with what the test does. That is what pushed it past the flaky-check 180s limit.
SYSTEM FLUSH LOGS query_log;

-- Columns: (any cache hit), (read no marks at all), (a projection served the read). A prime run is
-- 0 0, a reuse run that hit the cache and pruned everything is 1 1.
SELECT
    log_comment,
    ProfileEvents['QueryConditionCacheHits'] > 0,
    ProfileEvents['SelectedMarks'] = 0,
    arrayExists(x -> x LIKE '%.p' OR x LIKE '%.pn', projections)
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 300
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment LIKE '05045_assert_%'
    AND log_comment NOT IN ('05045_assert_proj_match_prime', '05045_assert_proj_match_reuse')
ORDER BY event_time_microseconds;

-- The matching needle is the one case that cannot assert "no marks at all": the chunk holding the
-- match is not fully filtered, and the `FilterTransform` write path has no per-mark information
-- inside a partially matching chunk. Columns: (reuse read fewer marks), (reuse hit the cache).
SELECT
    maxIf(marks, log_comment = '05045_assert_proj_match_prime') > maxIf(marks, log_comment = '05045_assert_proj_match_reuse'),
    minIf(hits, log_comment = '05045_assert_proj_match_reuse') > 0
FROM
(
    SELECT
        log_comment,
        ProfileEvents['SelectedMarks'] AS marks,
        ProfileEvents['QueryConditionCacheHits'] AS hits
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 300
        AND type = 'QueryFinish'
        AND current_database = currentDatabase()
        AND log_comment IN ('05045_assert_proj_match_prime', '05045_assert_proj_match_reuse')
);

DROP TABLE t_qcc_proj;
