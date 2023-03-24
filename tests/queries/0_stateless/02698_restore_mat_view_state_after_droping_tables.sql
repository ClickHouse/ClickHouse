DROP DATABASE IF EXISTS test_db;

SET skip_materialized_view_checking_if_source_table_not_exist = 1;
SET send_logs_level = 'fatal';

CREATE DATABASE test_db;

CREATE TABLE test_db.table (n Int32, s String) ENGINE MergeTree PARTITION BY n ORDER BY n;

CREATE TABLE test_db.mview_backend (n Int32, n2 Int64) ENGINE MergeTree PARTITION BY n ORDER BY n;

CREATE MATERIALIZED VIEW test_db.mview TO test_db.mview_backend AS SELECT n, n * n AS "n2" FROM test_db.table;

DROP TABLE test_db.table;

DETACH TABLE test_db.mview;

ATTACH TABLE test_db.mview;

CREATE TABLE test_db.table (n Int32, s String) ENGINE MergeTree PARTITION BY n ORDER BY n;

INSERT INTO test_db.table VALUES (3,'some_val');

SELECT n,s  FROM test_db.table ORDER BY n;
SELECT n,n2 FROM test_db.mview ORDER by n;

DROP DATABASE test_db;