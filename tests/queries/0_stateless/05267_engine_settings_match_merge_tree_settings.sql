-- `system.engine_settings` describes every engine of the `MergeTree` family with the same server-level instance
-- `system.merge_tree_settings` reads, and every replicated one with the instance `system.replicated_merge_tree_settings`
-- reads, so each such engine has to report exactly the rows of the matching table, in every column the two share.
-- Both directions: a row missing from `system.engine_settings` counts as much as one it adds.
-- The `Shared` engines of a private build are left out: nothing here says which of the two tables they match.

DROP VIEW IF EXISTS expected_rows;
DROP VIEW IF EXISTS engine_rows;

CREATE VIEW expected_rows AS
    SELECT e.engine AS engine, s.name AS name, s.value AS value, s.`default` AS `default`, s.changed AS changed,
        s.description AS description, s.min AS min, s.max AS max, s.disallowed_values AS disallowed_values,
        s.readonly AS readonly, s.type AS type, s.is_obsolete AS is_obsolete, s.tier AS tier, s.source AS source,
        s.alias_for AS alias_for
    FROM (SELECT name AS engine, startsWith(name, 'Replicated') AS replicated FROM system.table_engines
          WHERE endsWith(name, 'MergeTree') AND NOT startsWith(name, 'Shared')) AS e
    INNER JOIN (SELECT 0 AS replicated, * FROM system.merge_tree_settings
                UNION ALL
                SELECT 1 AS replicated, * FROM system.replicated_merge_tree_settings) AS s
    ON e.replicated = s.replicated;

CREATE VIEW engine_rows AS
    SELECT engine, name, value, `default`, changed, description, min, max, disallowed_values,
        readonly, type, is_obsolete, tier, source, alias_for
    FROM system.engine_settings
    WHERE endsWith(engine, 'MergeTree') AND NOT startsWith(engine, 'Shared');

SELECT '-- both families are compared, each engine with rows';
SELECT startsWith(engine, 'Replicated') AS replicated, count(DISTINCT engine) > 1, min(rows) > 100
FROM (SELECT engine, count() AS rows FROM engine_rows GROUP BY engine)
GROUP BY replicated ORDER BY replicated;

SELECT '-- rows only in system.engine_settings';
SELECT engine, name FROM (SELECT * FROM engine_rows EXCEPT SELECT * FROM expected_rows) ORDER BY engine, name;

SELECT '-- rows only in system.merge_tree_settings or system.replicated_merge_tree_settings';
SELECT engine, name FROM (SELECT * FROM expected_rows EXCEPT SELECT * FROM engine_rows) ORDER BY engine, name;

DROP VIEW expected_rows;
DROP VIEW engine_rows;
