-- Regression test for the `target_database` / `target_table` columns on
-- `system.tables`. The `/schema` web UI and any external consumer relies on
-- these fields to build MV target edges instead of regex-parsing
-- `create_table_query`, so silent breakage here would lose those edges.

DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS mv_to_target;
DROP TABLE IF EXISTS mv_with_inner;
DROP TABLE IF EXISTS plain;

CREATE TABLE src   (x UInt64) ENGINE = Memory;
CREATE TABLE dst   (x UInt64) ENGINE = Memory;
CREATE TABLE plain (x UInt64) ENGINE = Memory;

-- Explicit `TO target` form: should point at the user-named destination.
CREATE MATERIALIZED VIEW mv_to_target TO dst AS SELECT x FROM src;

-- Implicit-inner form: should point at the `.inner_id.<uuid>` table the engine
-- created next to the view. We don't pin the UUID, just assert the prefix.
CREATE MATERIALIZED VIEW mv_with_inner ENGINE = Memory AS SELECT x FROM src;

SELECT '-- mv_to_target points at the explicit destination';
SELECT name, target_database, target_table
FROM system.tables
WHERE database = currentDatabase() AND name = 'mv_to_target';

SELECT '-- mv_with_inner points at an `.inner_id.<uuid>` table in the same database';
SELECT
    name,
    target_database,
    startsWith(target_table, '.inner_id.') AS target_is_inner_id
FROM system.tables
WHERE database = currentDatabase() AND name = 'mv_with_inner';

SELECT '-- non-MV tables have empty target_database / target_table';
SELECT name, target_database, target_table
FROM system.tables
WHERE database = currentDatabase() AND name IN ('src', 'dst', 'plain')
ORDER BY name;

DROP TABLE mv_to_target;
DROP TABLE mv_with_inner;
DROP TABLE src;
DROP TABLE dst;
DROP TABLE plain;
