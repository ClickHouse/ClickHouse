-- Test: exercises `doSettingsSanityCheckClamp` clamping of `max_threads` when the
-- requested value exceeds 256 * getNumberOfCPUCoresToUse().
-- Covers: src/Core/SettingsQuirks.cpp:114-118 — the `max_threads` clamping branch.
-- The existing sanity-check test (02994_sanity_check_settings.sql) only exercises the
-- `max_block_size` clamp path; this test exercises the parallel `max_threads` branch
-- (its own test was previously commented out in 02994 because actually executing the query
-- with millions of threads was too costly for CI; we only verify the SETTING value here).

SET send_logs_level = 'error';

SET max_threads = 9223372036854775807;
-- The clamp must reduce the requested 2^63-1 to EXACTLY 256 * getNumberOfCPUCoresToUse().
-- The core count is derived at runtime from the still-visible `default` column (`auto(N)`),
-- which is unaffected by the SET above, so the bound tracks the real clamp contract on any
-- host instead of relying on a hardcoded threshold. The only literal is the documented 256
-- multiplier from src/Core/SettingsQuirks.cpp:113.
SELECT
    toUInt64(value) = 256 * toUInt64(extract(default, 'auto\\(([0-9]+)\\)')) AS clamped_to_256x_cores,
    toUInt64(value) < toUInt64(9223372036854775807) AS reduced_from_requested
FROM system.settings
WHERE name = 'max_threads';
