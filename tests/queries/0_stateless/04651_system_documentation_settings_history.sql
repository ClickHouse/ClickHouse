-- The documentation of a setting in `system.documentation` (the source of the built-in `/docs` Web UI and of the
-- `help` command) carries the history of the changes of its default value: the version in which the setting was
-- introduced and every later change of the default, with the reason for the change. The history is the same data
-- that backs the `compatibility` setting and `system.settings_changes`.

-- A setting has a history section exactly when its default value has recorded changes, and never otherwise.
WITH changed AS (SELECT DISTINCT arrayJoin(tupleElement(changes, 'name')) AS name FROM system.settings_changes WHERE type = 'Session')
SELECT count() FROM system.documentation
WHERE type = 'Setting' AND (name IN (SELECT name FROM changed)) != (position(description, '**History**') > 0);

WITH changed AS (SELECT DISTINCT arrayJoin(tupleElement(changes, 'name')) AS name FROM system.settings_changes WHERE type = 'MergeTree')
SELECT count() FROM system.documentation
WHERE type = 'MergeTree Setting' AND (name IN (SELECT name FROM changed)) != (position(description, '**History**') > 0);

-- Server settings are not covered by the `compatibility` setting, so no history of their changes is recorded.
SELECT count() FROM system.documentation WHERE type = 'Server Setting' AND position(description, '**History**') > 0;

-- The history lists exactly one item per recorded change of the setting.
SELECT count() FROM system.documentation AS d
INNER JOIN
(
    SELECT arrayJoin(tupleElement(changes, 'name')) AS name, count() AS recorded
    FROM system.settings_changes WHERE type = 'Session' GROUP BY name
) AS c USING (name)
WHERE d.type = 'Setting'
  AND length(splitByString('\n- **', substring(d.description, position(d.description, '**History**')))) - 1 != c.recorded;

-- The changes are listed newest first, so the most recent one comes right after the section header.
SELECT count() FROM system.documentation AS d
INNER JOIN
(
    SELECT arrayJoin(tupleElement(changes, 'name')) AS name,
           argMax(version, arrayMap(x -> toUInt32(x), splitByChar('.', version))) AS newest
    FROM system.settings_changes WHERE type = 'Session' GROUP BY name
) AS c USING (name)
WHERE d.type = 'Setting' AND position(d.description, '**History**\n\n- **' || c.newest || '** ') = 0;

-- The version in which the setting was introduced is called out separately, above the history.
SELECT count() > 100 FROM system.documentation WHERE type = 'Setting' AND position(description, '**Introduced in:** v') > 0;
SELECT count() > 0 FROM system.documentation WHERE type = 'MergeTree Setting' AND position(description, '**Introduced in:** v') > 0;

-- The full documentation of a setting: the description, the type, the default value, and the history.
-- `async_insert_max_data_size` is a long-standing setting whose default value was raised in 24.2.
SELECT description FROM system.documentation WHERE type = 'Setting' AND name = 'async_insert_max_data_size';

-- An alias is introduced in a particular version and has a history of its own, distinct from the history of the
-- setting it resolves to.
SELECT description FROM system.documentation WHERE type = 'Setting' AND name = 'enable_analyzer';
