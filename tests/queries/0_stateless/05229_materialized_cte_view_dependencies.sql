-- A name declared by a `WITH` list is not a table, so a stored view must not record a referential
-- dependency on a table of that name: the CTE hides it.
-- https://github.com/ClickHouse/ClickHouse/issues/113711
-- The real dependency (the table the CTE body reads) must survive, so the two `DROP`s below differ.

SET enable_materialized_cte = 1;

DROP TABLE IF EXISTS v_dep_113711b, v_dep_in_113711b, v_dep_plain_113711b, mv_dep_113711b;
DROP TABLE IF EXISTS src_dep_113711b, c_dep_113711b, mv_src_dep_113711b, mv_c_dep_113711b, mv_dst_dep_113711b;

CREATE TABLE src_dep_113711b (id UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO src_dep_113711b VALUES (1), (2), (3);
-- A real table carrying the CTE's name: rows 100, 200 below would mean a reference bound to it.
CREATE TABLE c_dep_113711b (id UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO c_dep_113711b VALUES (100), (200);

CREATE VIEW v_dep_113711b AS WITH c_dep_113711b AS MATERIALIZED (SELECT id FROM src_dep_113711b) SELECT * FROM c_dep_113711b;
CREATE VIEW v_dep_in_113711b AS WITH c_dep_113711b AS MATERIALIZED (SELECT id FROM src_dep_113711b) SELECT id FROM src_dep_113711b WHERE id IN c_dep_113711b;
CREATE VIEW v_dep_plain_113711b AS WITH c_dep_113711b AS (SELECT id FROM src_dep_113711b) SELECT * FROM c_dep_113711b;

SELECT '-- every view reads the CTE, not the same-named table';
SELECT * FROM v_dep_113711b ORDER BY id;
SELECT * FROM v_dep_in_113711b ORDER BY id;
SELECT * FROM v_dep_plain_113711b ORDER BY id;

SELECT '-- the CTE name is not a dependency of any of the three views';
-- Only the two `MATERIALIZED`-CTE views can record such a dependency in this shape: a fresh `CREATE`
-- expands a plain CTE before the dependency visitor sees the definition, so the plain-CTE case needs a
-- server restart and lives in
-- `tests/integration/test_materialized_cte_view_legacy_metadata::test_plain_cte_dependency_after_restart`.
DROP TABLE c_dep_113711b SETTINGS check_referential_table_dependencies = 1;
SELECT * FROM v_dep_113711b ORDER BY id;

SELECT '-- the table the CTE body reads is a dependency';
DROP TABLE src_dep_113711b SETTINGS check_referential_table_dependencies = 1; -- { serverError HAVE_DEPENDENT_OBJECTS }
DROP TABLE v_dep_113711b, v_dep_in_113711b, v_dep_plain_113711b;
DROP TABLE src_dep_113711b SETTINGS check_referential_table_dependencies = 1;

SELECT '-- the same for a materialized view writing to a target table';
CREATE TABLE mv_src_dep_113711b (id UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE mv_c_dep_113711b (id UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE mv_dst_dep_113711b (id UInt32) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW mv_dep_113711b TO mv_dst_dep_113711b AS
WITH mv_c_dep_113711b AS MATERIALIZED (SELECT id FROM mv_src_dep_113711b) SELECT id FROM mv_c_dep_113711b;
INSERT INTO mv_src_dep_113711b VALUES (1), (2);
SELECT * FROM mv_dst_dep_113711b ORDER BY id;
DROP TABLE mv_c_dep_113711b SETTINGS check_referential_table_dependencies = 1;
DROP TABLE mv_src_dep_113711b SETTINGS check_referential_table_dependencies = 1; -- { serverError HAVE_DEPENDENT_OBJECTS }
DROP TABLE mv_dst_dep_113711b SETTINGS check_referential_table_dependencies = 1; -- { serverError HAVE_DEPENDENT_OBJECTS }

DROP TABLE mv_dep_113711b;
DROP TABLE mv_dst_dep_113711b, mv_src_dep_113711b;

SELECT '-- a recursive CTE name does not hide a same-named table in a sibling branch';
DROP TABLE IF EXISTS v_rec_113711b;
DROP TABLE IF EXISTS r_rec_113711b;
CREATE TABLE r_rec_113711b (x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO r_rec_113711b VALUES (100);
CREATE VIEW v_rec_113711b AS SELECT x FROM (WITH RECURSIVE r_rec_113711b AS (SELECT 1 AS x UNION ALL SELECT x + 1 FROM r_rec_113711b WHERE x < 3) SELECT x FROM r_rec_113711b) UNION ALL SELECT x FROM r_rec_113711b;
SELECT * FROM v_rec_113711b ORDER BY x;
DROP TABLE r_rec_113711b SETTINGS check_referential_table_dependencies = 1; -- { serverError HAVE_DEPENDENT_OBJECTS }
DROP TABLE v_rec_113711b;
DROP TABLE r_rec_113711b;

SELECT '-- a CTE name in a dictionary query does not hide a same-named table in a sibling branch';
-- The names of the query resolve in the server's default database, so the edge is checked by name.
DROP DICTIONARY IF EXISTS d_dict_113711b;
CREATE DICTIONARY d_dict_113711b (x UInt8) PRIMARY KEY x
SOURCE(CLICKHOUSE(QUERY 'SELECT x FROM (WITH c_dict_113711b AS (SELECT 1 AS x) SELECT x FROM c_dict_113711b) UNION ALL SELECT x FROM c_dict_113711b'))
LIFETIME(0) LAYOUT(HASHED());
SELECT loading_dependencies_table FROM system.tables WHERE database = currentDatabase() AND name = 'd_dict_113711b';
DROP DICTIONARY d_dict_113711b;
