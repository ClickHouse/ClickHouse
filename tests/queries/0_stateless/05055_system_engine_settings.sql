-- Structure of the table.
SELECT name, type FROM system.columns WHERE database = 'system' AND table = 'engine_settings' ORDER BY position;

-- Engines with settings are present.
SELECT count() > 0 FROM system.engine_settings WHERE engine_name = 'MergeTree';
SELECT count() > 0 FROM system.engine_settings WHERE engine_name = 'Memory';
SELECT count(DISTINCT engine_name) > 3 FROM system.engine_settings;

-- Metadata of a known setting of an engine that has no server-level instance:
-- its value is its default and nothing is changed.
SELECT engine_name, name, value, `default`, changed, type, is_obsolete, tier
FROM system.engine_settings WHERE engine_name = 'Memory' AND name = 'compress';

-- `MergeTree` reports the settings the server actually uses, so the rows are exactly
-- `system.merge_tree_settings` - which is what makes that table expressible as a view.
SELECT count() FROM (
    SELECT name, value, `default`, changed, min, max, disallowed_values, readonly, type, is_obsolete, tier
    FROM system.engine_settings WHERE engine_name = 'MergeTree'
    EXCEPT
    SELECT name, value, `default`, changed, min, max, disallowed_values, readonly, type, is_obsolete, tier
    FROM system.merge_tree_settings);

-- Engines sharing a settings struct report the same set of settings.
SELECT count() FROM (
    SELECT name FROM system.engine_settings WHERE engine_name = 'MergeTree'
    EXCEPT
    SELECT name FROM system.engine_settings WHERE engine_name = 'ReplicatedMergeTree');

-- An engine that rejects a SETTINGS clause must not advertise settings.
SELECT count() FROM system.engine_settings AS s
INNER JOIN (SELECT name FROM system.table_engines WHERE NOT supports_settings) AS e
ON e.name = s.engine_name;

-- Every setting must render; a value with no string form used to throw.
SELECT count() > 0 FROM system.engine_settings WHERE name = 'storage_catalog_type';
