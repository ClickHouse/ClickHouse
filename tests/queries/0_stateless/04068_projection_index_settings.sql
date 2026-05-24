----------------------------------------------------------------------
-- 1. Basic INDEX projection with index_granularity override
----------------------------------------------------------------------

DROP TABLE IF EXISTS t_proj_settings;

CREATE TABLE t_proj_settings
(
    id UInt64,
    region String,
    PROJECTION p_gran2 INDEX region TYPE basic WITH SETTINGS (index_granularity = 2, index_granularity_bytes = 999999999)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0,
         min_bytes_for_full_part_storage = 0, enable_vertical_merge_algorithm = 0;

INSERT INTO t_proj_settings SELECT number, toString(number % 100) FROM numbers(100);

-- With index_granularity = 2, we expect 51 marks (100 rows / 2 + 1)
SELECT 'p_gran2 marks:', marks FROM system.projection_parts
WHERE active AND database = currentDatabase() AND table = 't_proj_settings' AND name = 'p_gran2';

-- Verify system.projections shows the user-specified settings
SELECT 'projections after create:', name, settings FROM system.projections
WHERE database = currentDatabase() AND table = 't_proj_settings' ORDER BY name;

----------------------------------------------------------------------
-- 2. ALTER ADD projection with settings override + MATERIALIZE
----------------------------------------------------------------------

ALTER TABLE t_proj_settings ADD PROJECTION p_gran10 INDEX region TYPE basic WITH SETTINGS (index_granularity = 10, index_granularity_bytes = 999999999);

ALTER TABLE t_proj_settings MATERIALIZE PROJECTION p_gran10 SETTINGS mutations_sync = 1;

-- With index_granularity = 10, we expect 11 marks (100 rows / 10 + 1)
SELECT 'p_gran10 marks:', marks FROM system.projection_parts
WHERE active AND database = currentDatabase() AND table = 't_proj_settings' AND name = 'p_gran10';

SELECT 'projections after alter:', name, settings FROM system.projections
WHERE database = currentDatabase() AND table = 't_proj_settings' ORDER BY name;

----------------------------------------------------------------------
-- 3. DETACH/ATTACH preserves projection settings
----------------------------------------------------------------------

DETACH TABLE t_proj_settings SYNC;
ATTACH TABLE t_proj_settings;

SELECT 'projections after reattach:', name, settings FROM system.projections
WHERE database = currentDatabase() AND table = 't_proj_settings' ORDER BY name;

DROP TABLE t_proj_settings;

----------------------------------------------------------------------
-- 4. Projection-level min_bytes_for_wide_part overrides parent table
----------------------------------------------------------------------

DROP TABLE IF EXISTS t_proj_wide;

-- Parent table forces Wide format (min_bytes_for_wide_part = 0).
-- Projection overrides to force Compact format (min_bytes_for_wide_part = very large).
CREATE TABLE t_proj_wide
(
    id UInt64,
    region String,
    PROJECTION p_compact INDEX region TYPE basic WITH SETTINGS (min_bytes_for_wide_part = 1000000000)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO t_proj_wide SELECT number, toString(number % 100) FROM numbers(100);

-- Projection has min_bytes_for_wide_part = 1000000000, so for 100 rows it should be Compact
-- (regardless of what the parent table's random settings may be).
SELECT 'projection part_type:', part_type FROM system.projection_parts
WHERE active AND database = currentDatabase() AND table = 't_proj_wide' AND name = 'p_compact';

DROP TABLE t_proj_wide;

----------------------------------------------------------------------
-- 5. Projection-level min_compress_block_size overrides parent table
----------------------------------------------------------------------

DROP TABLE IF EXISTS t_proj_compress;

-- Parent table uses default min_compress_block_size (65536).
-- Projection overrides to a different value. Verify via system.projections settings map.
CREATE TABLE t_proj_compress
(
    id UInt64,
    region String,
    PROJECTION p_compress INDEX region TYPE basic WITH SETTINGS (min_compress_block_size = 1234567)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_full_part_storage = 0;

SELECT 'compress settings:', name, settings['min_compress_block_size'] FROM system.projections
WHERE database = currentDatabase() AND table = 't_proj_compress' ORDER BY name;

DROP TABLE t_proj_compress;

----------------------------------------------------------------------
-- 6. Disallowed settings are rejected for INDEX projections
----------------------------------------------------------------------

DROP TABLE IF EXISTS t_bad_settings;

CREATE TABLE t_bad_settings (id UInt64, region String)
ENGINE = MergeTree ORDER BY id;

-- Unknown/disallowed setting
ALTER TABLE t_bad_settings ADD PROJECTION p_bad INDEX region TYPE basic WITH SETTINGS (max_parts_to_merge_at_once = 5); -- { serverError BAD_ARGUMENTS }

-- `index_granularity_bytes = 0` forces non-adaptive granularity, which projections do not support.
ALTER TABLE t_bad_settings ADD PROJECTION p_bad INDEX region TYPE basic WITH SETTINGS (index_granularity_bytes = 0); -- { serverError BAD_ARGUMENTS }

-- Both `index_granularity` and `index_granularity_bytes` set to zero leaves granule sizing with no driver.
ALTER TABLE t_bad_settings ADD PROJECTION p_bad INDEX region TYPE basic WITH SETTINGS (index_granularity = 0, index_granularity_bytes = 0); -- { serverError BAD_ARGUMENTS }

DROP TABLE t_bad_settings;

----------------------------------------------------------------------
-- 7. Non-adaptive table rejects INDEX projections with granularity overrides
----------------------------------------------------------------------

DROP TABLE IF EXISTS t_fixed_gran;

CREATE TABLE t_fixed_gran (id UInt64, region String)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity_bytes = 0;

ALTER TABLE t_fixed_gran ADD PROJECTION p_bad INDEX region TYPE basic WITH SETTINGS (index_granularity = 2); -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE t_fixed_gran;
