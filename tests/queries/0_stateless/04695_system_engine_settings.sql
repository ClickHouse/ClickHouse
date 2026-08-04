-- Tags: no-fasttest
-- Verify system.engine_settings table exists and has correct structure
SELECT count() > 0 FROM system.engine_settings;

-- Verify required columns exist
SELECT name, type FROM system.columns WHERE database = 'system' AND table = 'engine_settings' ORDER BY position;

-- Verify MergeTree settings are present
SELECT count() > 0 FROM system.engine_settings WHERE engine_name = 'MergeTree';

-- Verify Memory settings are present
SELECT count() > 0 FROM system.engine_settings WHERE engine_name = 'Memory';

-- Verify basic settings metadata for a known MergeTree setting
SELECT engine_name, name, changed, readonly, is_obsolete FROM system.engine_settings WHERE name = 'index_granularity' AND engine_name = 'MergeTree';

-- Verify basic settings metadata for a known Memory setting
SELECT engine_name, name, default, type FROM system.engine_settings WHERE name = 'compress' AND engine_name = 'Memory';

-- Verify multiple engines have settings
SELECT count(DISTINCT engine_name) > 3 FROM system.engine_settings;
