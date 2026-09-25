-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/67601
-- The plain-view dependency graph behind `system.tables.dependencies_database` and
-- `system.tables.dependencies_table` is keyed by database and table name. An `ALTER`
-- on a view must not re-key its graph node by UUID: otherwise dropping and recreating
-- the view under the same name and altering it again removes the node as a name
-- conflict, silently losing the ordinary views that depend on it.

-- Dropping and recreating a table that is still referenced leaves a node with a stale UUID in
-- the `ReferentialDeps` and `ViewDeps` graphs, so the next update of that node logs a name
-- conflict. That is pre-existing behaviour of `TablesDependencyGraph`, unrelated to what this
-- test checks, and it is reproducible with two materialized views on an unpatched server.
SET send_logs_level = 'error';

DROP TABLE IF EXISTS plain_view_recreate_source;
DROP VIEW IF EXISTS plain_view_recreate_v1;
DROP VIEW IF EXISTS plain_view_recreate_v2;

CREATE TABLE plain_view_recreate_source (id UInt64) ENGINE = Memory;
CREATE VIEW plain_view_recreate_v1 AS SELECT * FROM plain_view_recreate_source;
CREATE VIEW plain_view_recreate_v2 AS SELECT * FROM plain_view_recreate_v1;

SELECT 'initial', dependencies_table FROM system.tables
WHERE database = currentDatabase() AND name = 'plain_view_recreate_v1';

ALTER TABLE plain_view_recreate_v1 MODIFY COMMENT 'first';

SELECT 'after alter', dependencies_table FROM system.tables
WHERE database = currentDatabase() AND name = 'plain_view_recreate_v1';

-- Recreate the view under the same name: `plain_view_recreate_v2` still depends on it.
DROP VIEW plain_view_recreate_v1;
CREATE VIEW plain_view_recreate_v1 AS SELECT * FROM plain_view_recreate_source;

ALTER TABLE plain_view_recreate_v1 MODIFY COMMENT 'second';

SELECT 'after recreate and alter', dependencies_table FROM system.tables
WHERE database = currentDatabase() AND name = 'plain_view_recreate_v1';

-- The source table must still report both views as its dependents.
SELECT 'source', arraySort(dependencies_table) FROM system.tables
WHERE database = currentDatabase() AND name = 'plain_view_recreate_source';

DROP VIEW plain_view_recreate_v2;
DROP VIEW plain_view_recreate_v1;
DROP TABLE plain_view_recreate_source;
