-- The backport of the setting must be recorded in the `26.8` block of the settings history, as on the
-- 26.8 branch, not in the master block. Otherwise a `compatibility` of 26.8 or 26.9 turns the fix off.

SET compatibility = '26.9';
SELECT value FROM system.settings WHERE name = 'iceberg_tolerate_conflicting_manifest_schemas';

SET compatibility = '26.8';
SELECT value FROM system.settings WHERE name = 'iceberg_tolerate_conflicting_manifest_schemas';

SET compatibility = '26.7';
SELECT value FROM system.settings WHERE name = 'iceberg_tolerate_conflicting_manifest_schemas';
