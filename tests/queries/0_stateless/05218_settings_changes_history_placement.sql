-- Tags: no-random-settings
-- no-random-settings: the test asserts the defaults a `compatibility` pin produces, and an
-- explicitly set setting is not affected by the pin.

-- A `compatibility` pin must reproduce the defaults the pinned release shipped: an entry recorded in
-- the wrong version block of `SettingsChangesHistory` reverts a default the pinned release had, or
-- keeps one it did not have.

SELECT 'shipped in 26.8';
SET compatibility = '26.8';
SELECT name, value FROM system.settings
WHERE name IN ('query_plan_optimize_lazy_materialization_for_object_storage',
               'distributed_cache_min_inflight_bytes_to_discard_connection_on_seek',
               'distributed_plan_workers_provisioning_timeout_ms')
ORDER BY name;

SELECT 'not yet in 26.7';
SET compatibility = '26.7';
SELECT name, value FROM system.settings
WHERE name IN ('query_plan_optimize_lazy_materialization_for_object_storage',
               'distributed_cache_min_inflight_bytes_to_discard_connection_on_seek',
               'distributed_plan_workers_provisioning_timeout_ms')
ORDER BY name;

SELECT 'also shipped in 26.8';
SET compatibility = '26.8';
SELECT value FROM system.settings WHERE name = 'query_plan_aggregation_bucket_top_k';
SET compatibility = '26.7';
SELECT value FROM system.settings WHERE name = 'query_plan_aggregation_bucket_top_k';

SELECT 'enabled in 26.1';
SET compatibility = '26.1';
SELECT value FROM system.settings WHERE name = 'use_skip_indexes_on_data_read';
SET compatibility = '25.12';
SELECT value FROM system.settings WHERE name = 'use_skip_indexes_on_data_read';
