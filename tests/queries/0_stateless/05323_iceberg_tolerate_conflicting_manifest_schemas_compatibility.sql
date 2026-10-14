-- The backport of the setting must be recorded in the `26.8` block of the settings history, not in
-- the master block. Otherwise a `compatibility` of this release line turns the fix off.

SET compatibility = '26.8';
SELECT value FROM system.settings WHERE name = 'iceberg_tolerate_conflicting_manifest_schemas';

SET compatibility = '26.7';
SELECT value FROM system.settings WHERE name = 'iceberg_tolerate_conflicting_manifest_schemas';
