-- Tags: no-fasttest
--   No existing test covers combining projection INDEX TYPE syntax with WITH SETTINGS.

SET enable_analyzer=1;

DROP TABLE IF EXISTS test_idx_settings_cov;

-- 1. INDEX TYPE basic WITH SETTINGS (index_granularity = 2)
CREATE TABLE test_idx_settings_cov (
    id UInt64,
    region String,
    PROJECTION region_proj INDEX region TYPE basic WITH SETTINGS (index_granularity = 2)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0,
         min_bytes_for_full_part_storage = 0, enable_vertical_merge_algorithm = 0;

INSERT INTO test_idx_settings_cov SELECT number, toString(number % 10) FROM numbers(100);

-- Verify projection has custom granularity (should have ~51 marks for 100 rows with granularity 2)
SELECT marks FROM system.projection_parts WHERE active AND database = currentDatabase() AND table = 'test_idx_settings_cov' AND name = 'region_proj';

-- Verify settings serialized
SELECT name, settings FROM system.projections WHERE database = currentDatabase() AND table = 'test_idx_settings_cov' ORDER BY name;

-- Verify DETACH/ATTACH preserves settings
DETACH TABLE test_idx_settings_cov SYNC;
ATTACH TABLE test_idx_settings_cov;

SELECT name, settings FROM system.projections WHERE database = currentDatabase() AND table = 'test_idx_settings_cov' ORDER BY name;

-- Verify projection still works after ATTACH
SELECT count() FROM test_idx_settings_cov WHERE region = '5';

DROP TABLE IF EXISTS test_idx_settings_cov;
