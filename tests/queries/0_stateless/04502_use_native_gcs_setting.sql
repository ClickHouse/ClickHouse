-- The `use_native_gcs` setting selects the native Google Cloud SDK backend for the `gcs`
-- table function / `GCS` engine. Here we only verify that the setting is registered, defaults
-- to off, and can be toggled (the native path itself needs a real GCS endpoint / emulator and is
-- exercised by integration tests). This runs in any build because the setting is declared
-- unconditionally, independent of whether the google-cloud-cpp SDK is compiled in.

SELECT name FROM system.settings WHERE name = 'use_native_gcs';
SELECT toUInt8(getSetting('use_native_gcs'));
SET use_native_gcs = 1;
SELECT toUInt8(getSetting('use_native_gcs'));
SET use_native_gcs = 0;
SELECT toUInt8(getSetting('use_native_gcs'));
