DROP DATABASE IF EXISTS test_db;

SET ignore_inaccessible_tables_mat_view_attach = 1;
SET send_logs_level = 'fatal';

CREATE DATABASE test_db;

CREATE TABLE test_db.table (n Int32, s String) ENGINE MergeTree PARTITION BY n ORDER BY n;

CREATE TABLE test_db.mview_backend (n Int32, n2 Int64) ENGINE MergeTree PARTITION BY n ORDER BY n;

CREATE MATERIALIZED VIEW test_db.mview TO test_db.mview_backend AS SELECT n, n * n AS "n2" FROM test_db.table;

DROP TABLE test_db.table;

DETACH TABLE test_db.mview;

ATTACH TABLE test_db.mview;

SET ignore_inaccessible_tables_mat_view_attach = 0;

DROP DATABASE test_db;

