-- A view definition that turns `enable_global_with_statement` off inside itself hides its own CTE
-- from the nested subquery, so the reference there is a table name and is qualified with the
-- database of the `CREATE`: the stored definition must not depend on the reader's session.
-- https://github.com/ClickHouse/ClickHouse/issues/113711
-- The rule does not depend on the CTE being `MATERIALIZED`: a plain CTE hidden the same way is
-- qualified too, so the view answers what the same query answers outside a view.

SET enable_materialized_cte = 1;

DROP TABLE IF EXISTS v_nested_set_113711b, v_mixed_set_113711b, v_plain_set_113711b, r_nested_113711b, r_plain_113711b;

CREATE TABLE r_nested_113711b (x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO r_nested_113711b VALUES (7);

CREATE VIEW v_nested_set_113711b AS
WITH r_nested_113711b AS MATERIALIZED (SELECT 1 AS x)
SELECT * FROM (SELECT * FROM r_nested_113711b SETTINGS enable_global_with_statement = 0);

SELECT '-- the nested reference is stored qualified';
SELECT replaceAll(replaceRegexpOne(create_table_query, '.*AS WITH', 'WITH'), currentDatabase(), 'db') FROM system.tables WHERE database = currentDatabase() AND name = 'v_nested_set_113711b';

SELECT '-- and reads the table, as in a plain query with the setting off';
SELECT * FROM v_nested_set_113711b;

SELECT '-- a reference the definition does not hide stays bare, in the same definition';
CREATE VIEW v_mixed_set_113711b AS
WITH r_nested_113711b AS MATERIALIZED (SELECT 1 AS x)
SELECT a.x AS from_cte, b.x AS from_table
FROM r_nested_113711b AS a, (SELECT * FROM r_nested_113711b SETTINGS enable_global_with_statement = 0) AS b;
SELECT replaceAll(replaceRegexpOne(create_table_query, '.*AS WITH', 'WITH'), currentDatabase(), 'db') FROM system.tables WHERE database = currentDatabase() AND name = 'v_mixed_set_113711b';
SELECT * FROM v_mixed_set_113711b;

SELECT '-- the same for a plain CTE';
-- 9 distinguishes the table from the CTE's 1, and is what the same query returns outside a view.
CREATE TABLE r_plain_113711b (x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO r_plain_113711b VALUES (9);
CREATE VIEW v_plain_set_113711b AS
WITH r_plain_113711b AS (SELECT 1 AS x)
SELECT * FROM (SELECT * FROM r_plain_113711b SETTINGS enable_global_with_statement = 0);
SELECT replaceAll(replaceRegexpOne(create_table_query, '.*AS WITH', 'WITH'), currentDatabase(), 'db') FROM system.tables WHERE database = currentDatabase() AND name = 'v_plain_set_113711b';
SELECT * FROM v_plain_set_113711b;
WITH r_plain_113711b AS (SELECT 1 AS x) SELECT * FROM (SELECT * FROM r_plain_113711b SETTINGS enable_global_with_statement = 0);

DROP TABLE v_plain_set_113711b, v_mixed_set_113711b, v_nested_set_113711b, r_plain_113711b, r_nested_113711b;

SELECT '-- a clause on the outer select hides the CTE from the nested subquery too';
-- 5 distinguishes the table from the CTE's 1.
DROP TABLE IF EXISTS v_outer_set_113711b, m_outer_113711b;
CREATE TABLE m_outer_113711b (x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO m_outer_113711b VALUES (5);
CREATE VIEW v_outer_set_113711b AS
WITH m_outer_113711b AS MATERIALIZED (SELECT 1 AS x)
SELECT * FROM (SELECT * FROM m_outer_113711b) SETTINGS enable_global_with_statement = 0;
SELECT replaceAll(replaceRegexpOne(create_table_query, '.*AS WITH', 'WITH'), currentDatabase(), 'db') FROM system.tables WHERE database = currentDatabase() AND name = 'v_outer_set_113711b';
SELECT * FROM v_outer_set_113711b;
DROP TABLE v_outer_set_113711b, m_outer_113711b;
