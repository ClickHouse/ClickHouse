-- Test: exercises `doSettingsSanityCheckClamp` clamping of `max_threads` when the
-- requested value exceeds 256 * getNumberOfCPUCoresToUse().
-- Covers: src/Core/SettingsQuirks.cpp:114-118 — the `max_threads` clamping branch.
-- The existing sanity-check test (02994_sanity_check_settings.sql) only exercises the
-- `max_block_size` clamp path; this test exercises the parallel `max_threads` branch
-- (its own test was previously commented out in 02994 because actually executing the query
-- with millions of threads was too costly for CI; we only verify the SETTING value here).

SET send_logs_level = 'error';

SET max_threads = 9223372036854775807;
-- After clamp, the effective `max_threads` value must be far below the requested 2^63-1.
SELECT toUInt64(value) < toUInt64(1000000000) FROM system.settings WHERE name = 'max_threads';
