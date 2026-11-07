-- `system.engine_settings` reports, for every engine of the `MergeTree` family, the server-level instance that
-- `system.merge_tree_settings` shows, and for every replicated one the instance `system.replicated_merge_tree_settings`
-- shows: the settings a new table starts from, with the current user's constraints. So each such engine has exactly
-- the rows of the matching table, in every column the two share. Both directions are checked.
-- The `Shared` engines of a private build are left out: nothing here says which of the two tables they match.

DROP VIEW IF EXISTS expected_rows;
DROP VIEW IF EXISTS engine_rows;

CREATE VIEW expected_rows AS
    SELECT e.engine_name AS engine_name, s.name AS name, s.value AS value, s.`default` AS `default`,
        s.changed AS changed, s.description AS description, s.min AS min, s.max AS max,
        s.disallowed_values AS disallowed_values, s.readonly AS readonly, s.type AS type,
        s.is_obsolete AS is_obsolete, s.tier AS tier
    FROM (SELECT name AS engine_name, startsWith(name, 'Replicated') AS replicated FROM system.table_engines
          WHERE endsWith(name, 'MergeTree') AND NOT startsWith(name, 'Shared')) AS e
    INNER JOIN (SELECT 0 AS replicated, * FROM system.merge_tree_settings
                UNION ALL
                SELECT 1 AS replicated, * FROM system.replicated_merge_tree_settings) AS s
    ON e.replicated = s.replicated;

CREATE VIEW engine_rows AS
    SELECT engine_name, name, value, `default`, changed, description, min, max, disallowed_values,
        readonly, type, is_obsolete, tier
    FROM system.engine_settings
    WHERE endsWith(engine_name, 'MergeTree') AND NOT startsWith(engine_name, 'Shared');

SELECT '-- both families are compared, each engine with rows';
SELECT startsWith(engine_name, 'Replicated') AS replicated, count() > 1, min(rows) > 100
FROM (SELECT engine_name, count() AS rows FROM engine_rows GROUP BY engine_name)
GROUP BY replicated ORDER BY replicated;

SELECT '-- rows only in system.engine_settings';
SELECT engine_name, name FROM (SELECT * FROM engine_rows EXCEPT SELECT * FROM expected_rows) ORDER BY engine_name, name;

SELECT '-- rows only in system.merge_tree_settings or system.replicated_merge_tree_settings';
SELECT engine_name, name FROM (SELECT * FROM expected_rows EXCEPT SELECT * FROM engine_rows) ORDER BY engine_name, name;

DROP VIEW expected_rows;
DROP VIEW engine_rows;
