-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database
-- Reads of projection index parts must never populate the columns cache: projection parts
-- share the projection name as their part name, which is not unique across parent parts,
-- so their entries would collide in the cache. `MergeTreeReadPoolProjectionIndex` therefore
-- gets no columns cache at all, which also means it needs no query-scoped write budget.

SET max_threads = 1;
SET enable_analyzer = 1;
SET optimize_use_projections = 1, optimize_use_projection_filtering = 1;
SET min_table_rows_to_use_projection_index = 0;

DROP TABLE IF EXISTS t_cc_proj_index;

CREATE TABLE t_cc_proj_index
(
    id UInt64,
    region String,
    payload String,
    PROJECTION region_proj INDEX region TYPE basic
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8, min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0, enable_vertical_merge_algorithm = 0;

-- `region = 'eu'` only in the first granule, so the projection index prunes almost
-- everything and is therefore chosen for read-time filtering.
INSERT INTO t_cc_proj_index SELECT number, if(number < 8, 'eu', 'us'), toString(number) FROM numbers(2048);

OPTIMIZE TABLE t_cc_proj_index FINAL;

-- Guard against the test going vacuous: the projection index must really be read.
-- Only the presence of the line is asserted, because its indentation differs between
-- read variants (with parallel replicas the remote plan is drawn inside a box).
SELECT count() > 0
FROM (EXPLAIN projections = 1 SELECT count(), sum(id) FROM t_cc_proj_index WHERE region = 'eu')
WHERE explain LIKE '%Projection has been analyzed and will be applied during reading%';

SYSTEM DROP COLUMNS CACHE;

-- The projection index prunes granules by `region`; the result is read from the parent part.
SELECT count(), sum(id) FROM t_cc_proj_index WHERE region = 'eu' SETTINGS use_columns_cache = 1;

-- The same query with the cache off must produce the same result.
SELECT count(), sum(id) FROM t_cc_proj_index WHERE region = 'eu' SETTINGS use_columns_cache = 0;

-- Nothing may be cached under the projection's name.
SELECT count() FROM system.columns_cache WHERE database = currentDatabase() AND part = 'region_proj';

-- A repeated run served (partly) from the cache must return the same result.
SELECT count(), sum(id) FROM t_cc_proj_index WHERE region = 'eu' SETTINGS use_columns_cache = 1;

-- A full scan of the same table (no projection index) does populate the cache, which shows
-- that the empty result above is the projection-part exclusion and not a disabled cache.
SELECT sum(id) FROM t_cc_proj_index SETTINGS use_columns_cache = 1, optimize_use_projections = 0;

SELECT count() > 0 FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_cc_proj_index';
SELECT count() FROM system.columns_cache WHERE database = currentDatabase() AND part = 'region_proj';

DROP TABLE t_cc_proj_index;
