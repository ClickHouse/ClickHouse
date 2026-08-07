drop table if exists t_table_select;
CREATE TABLE t_table_select (id UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_table_select (id) SELECT number FROM numbers(30);

CREATE TEMPORARY TABLE t_test (x UInt32, y Nullable(UInt32)) AS SELECT a.id, b.id FROM remote('127.0.0.{1,2}', currentDatabase(), t_table_select) AS a GLOBAL LEFT JOIN (SELECT id FROM remote('127.0.0.{1,2}', currentDatabase(), t_table_select) AS b WHERE (b.id % 10) = 0) AS b ON b.id = a.id SETTINGS join_use_nulls = 1;

select * from t_test order by x;

