-- Regression test: a table lists an ordinary view in `system.tables.dependencies_*` whenever the definition
-- of the view refers to it, not only when it is a `FROM` / `JOIN` table expression. The same references
-- already block `DROP TABLE` of the table through the referential dependencies, so both must agree.

DROP VIEW IF EXISTS carriers_view;
DROP DICTIONARY IF EXISTS carriers_dict;
DROP TABLE IF EXISTS carriers_from;
DROP TABLE IF EXISTS carriers_in;
DROP TABLE IF EXISTS carriers_join;
DROP TABLE IF EXISTS carriers_dict_source;

CREATE TABLE carriers_from (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE carriers_in (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE carriers_join (id UInt64, val UInt64) ENGINE = Join(ANY, LEFT, id);
CREATE TABLE carriers_dict_source (id UInt64, val UInt64) ENGINE = MergeTree ORDER BY id;
CREATE DICTIONARY carriers_dict (id UInt64, val UInt64) PRIMARY KEY id
    SOURCE(CLICKHOUSE(TABLE 'carriers_dict_source' DATABASE currentDatabase()))
    LAYOUT(FLAT()) LIFETIME(0);

CREATE VIEW carriers_view AS
    SELECT id, joinGet(carriers_join, 'val', id) AS j, dictGet(carriers_dict, 'val', id) AS d
    FROM carriers_from
    WHERE id IN carriers_in;

SELECT name, dependencies_database = [currentDatabase()], dependencies_table
FROM system.tables
WHERE database = currentDatabase() AND name IN ('carriers_from', 'carriers_in', 'carriers_join', 'carriers_dict', 'carriers_dict_source')
ORDER BY name;

-- Each of them blocks `DROP` while the view refers to it: the listed dependents and the guarded objects agree.
SET check_referential_table_dependencies = 1;
SET send_logs_level = 'fatal'; -- the expected errors below must not be sent to the client
DROP TABLE carriers_in; -- { serverError HAVE_DEPENDENT_OBJECTS }
DROP TABLE carriers_join; -- { serverError HAVE_DEPENDENT_OBJECTS }
DROP DICTIONARY carriers_dict; -- { serverError HAVE_DEPENDENT_OBJECTS }
SET send_logs_level = 'warning';

DROP VIEW carriers_view;

SELECT name, dependencies_table
FROM system.tables
WHERE database = currentDatabase() AND name IN ('carriers_from', 'carriers_in', 'carriers_join', 'carriers_dict')
ORDER BY name;

DROP TABLE carriers_in;
DROP TABLE carriers_join;
DROP DICTIONARY carriers_dict;
DROP TABLE carriers_dict_source;
DROP TABLE carriers_from;
