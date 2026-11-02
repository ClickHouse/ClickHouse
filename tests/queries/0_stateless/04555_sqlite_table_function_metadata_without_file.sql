-- Tags: no-fasttest
-- Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

-- A table defined over the `sqlite` table function with an explicit structure must not open (or create) the
-- database file for metadata-only access: `CREATE`, `ATTACH` and `system.tables` all work while the file is
-- missing, and only an actual read fails closed.

CREATE TABLE tf_missing (x Int32) AS sqlite('04555_missing_db_path', 'tbl');

SELECT name, engine, create_table_query FROM system.tables WHERE name = 'tf_missing' AND database = currentDatabase();

SELECT * FROM tf_missing; -- { serverError PATH_ACCESS_DENIED }

DETACH TABLE tf_missing;
ATTACH TABLE tf_missing;

SELECT name, engine FROM system.tables WHERE name = 'tf_missing' AND database = currentDatabase();

DROP TABLE tf_missing;
