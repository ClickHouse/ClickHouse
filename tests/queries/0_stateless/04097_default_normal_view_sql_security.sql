-- Tags: no-parallel

DROP TABLE IF EXISTS test_table;
DROP VIEW IF EXISTS test_view_default;
DROP VIEW IF EXISTS test_view_definer;
DROP VIEW IF EXISTS test_view_invoker;
DROP VIEW IF EXISTS test_view_none;

CREATE TABLE test_table (s String) ENGINE = MergeTree ORDER BY s;
INSERT INTO test_table VALUES ('hello'), ('world');

-- Test 1: default_normal_view_sql_security = DEFINER should be applied on CREATE VIEW
SELECT 'default_normal_view_sql_security = DEFINER';

SET default_normal_view_sql_security = 'DEFINER';
CREATE VIEW test_view_definer AS SELECT * FROM test_table;

SELECT create_table_query LIKE '%SQL SECURITY DEFINER%' AS has_definer FROM system.tables WHERE database = currentDatabase() AND name = 'test_view_definer';
SELECT definer != '' AS has_definer_user FROM system.tables WHERE database = currentDatabase() AND name = 'test_view_definer';

-- Test 2: default_normal_view_sql_security = INVOKER should be applied on CREATE VIEW
SELECT 'default_normal_view_sql_security = INVOKER';

SET default_normal_view_sql_security = 'INVOKER';
CREATE VIEW test_view_invoker AS SELECT * FROM test_table;

SELECT create_table_query LIKE '%SQL SECURITY INVOKER%' AS has_invoker FROM system.tables WHERE database = currentDatabase() AND name = 'test_view_invoker';

-- Test 3: default_normal_view_sql_security = NONE should be applied on CREATE VIEW
SELECT 'default_normal_view_sql_security = NONE';

SET default_normal_view_sql_security = 'NONE';
CREATE VIEW test_view_none AS SELECT * FROM test_table;

SELECT create_table_query LIKE '%SQL SECURITY NONE%' AS has_none FROM system.tables WHERE database = currentDatabase() AND name = 'test_view_none';

-- Test 4: Views created with defaults should work correctly
SELECT 'functional check';
SELECT count() FROM test_view_definer;
SELECT count() FROM test_view_invoker;
SELECT count() FROM test_view_none;

DROP VIEW test_view_definer;
DROP VIEW test_view_invoker;
DROP VIEW test_view_none;
DROP TABLE test_table;
