-- Tags: no-random-settings
-- no-random-settings: the test asserts the defaults a `compatibility` pin produces, and an explicitly set setting is not affected by the pin.

-- The change was backported to 26.8, so `compatibility = '26.8'` must keep the 26.8 default.
SET compatibility = '26.8';
SELECT value FROM system.settings WHERE name = 'iceberg_manifest_decode_concurrency';
SET compatibility = '26.7';
SELECT value FROM system.settings WHERE name = 'iceberg_manifest_decode_concurrency';
