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

-- Engines of one data lake family are backed by the same settings struct, so each must report the
-- same settings whichever storage backend it names. `DeltaLakeLocal` did not: it was registered
-- with the plain object storage predicate while its creator builds `DataLakeStorageSettings`.
-- Both answer 1 in a build without these engines, where the subquery is empty.
SELECT countDistinct(n) <= 1 FROM (
    SELECT count() AS n FROM system.engine_settings WHERE engine_name LIKE 'DeltaLake%' GROUP BY engine_name);
SELECT countDistinct(n) <= 1 FROM (
    SELECT count() AS n FROM system.engine_settings WHERE engine_name LIKE 'Iceberg%' GROUP BY engine_name);

-- Every engine that accepts a SETTINGS clause should be able to say which settings it accepts.
-- These six cannot: they keep no settings struct, so what a table of theirs reports comes from the
-- base implementation reading its stored definition. A name appearing here that is not one of the
-- six means an engine was added without being wired up.
SELECT name FROM system.table_engines
WHERE supports_settings AND name NOT IN (SELECT DISTINCT engine_name FROM system.engine_settings)
ORDER BY name;
