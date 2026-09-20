-- The `SETTINGS` clause of a stored definition is replayed by the qualification passes to predict
-- the `WITH` visibility of the nested `SELECT`, and a settings profile is replayed with it because a
-- profile can set that very setting. The pass that repairs stored metadata and the dependency graph
-- built at startup run that replay without executing the query, so a clause naming a profile this
-- server does not have - dropped, renamed, or never created on this replica - must not fail them:
-- the definition still attaches, unchanged.
-- The first profile leaves the setting at its default, so the nested `SELECT` sees the enclosing
-- common table expression and the rows below are the ones the same query gives without a profile.

DROP SETTINGS PROFILE IF EXISTS p_05234, p_05234_off;
CREATE SETTINGS PROFILE p_05234 SETTINGS enable_global_with_statement = 1;
CREATE SETTINGS PROFILE p_05234_off SETTINGS enable_global_with_statement = 0;

DROP TABLE IF EXISTS src;
CREATE TABLE src (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO src VALUES (1);

DROP VIEW IF EXISTS v_profile_clause;
DROP VIEW IF EXISTS v_profile_clause_off;
CREATE VIEW v_profile_clause AS
    WITH src AS (SELECT 7 AS id) SELECT id FROM (SELECT id FROM src SETTINGS profile = 'p_05234');
-- The second profile turns that setting off, the value that would decide the scope the other way,
-- so the walk has to reach the same clause with the profile gone and leave the definition as it is.
CREATE VIEW v_profile_clause_off AS
    WITH src AS (SELECT 7 AS id) SELECT id FROM (SELECT id FROM src SETTINGS profile = 'p_05234_off');

SELECT 'read, profile present', * FROM v_profile_clause;
SELECT 'definition', name, replaceAll(replaceAll(create_table_query, currentDatabase(), 'DB'), '\n', ' ')
FROM system.tables WHERE database = currentDatabase() AND name LIKE 'v_profile_clause%' ORDER BY name;

DROP SETTINGS PROFILE p_05234, p_05234_off;

DETACH TABLE v_profile_clause;
ATTACH TABLE v_profile_clause;
DETACH TABLE v_profile_clause_off;
ATTACH TABLE v_profile_clause_off;

SELECT 'attached, profiles gone', count() FROM system.tables
WHERE database = currentDatabase() AND name LIKE 'v_profile_clause%';
SELECT 'definition', name, replaceAll(replaceAll(create_table_query, currentDatabase(), 'DB'), '\n', ' ')
FROM system.tables WHERE database = currentDatabase() AND name LIKE 'v_profile_clause%' ORDER BY name;

CREATE SETTINGS PROFILE p_05234 SETTINGS enable_global_with_statement = 1;
CREATE SETTINGS PROFILE p_05234_off SETTINGS enable_global_with_statement = 0;
SELECT 'read, profiles back', * FROM v_profile_clause;

DROP SETTINGS PROFILE p_05234, p_05234_off;
DROP VIEW v_profile_clause;
DROP VIEW v_profile_clause_off;
DROP TABLE src;
