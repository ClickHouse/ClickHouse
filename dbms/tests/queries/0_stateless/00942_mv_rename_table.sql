DROP TABLE IF EXISTS test.src;
DROP TABLE IF EXISTS test.view_table;
DROP TABLE IF EXISTS test.new_view_table;

CREATE TABLE test.src (x UInt8) ENGINE = Null;

USE test;

CREATE MATERIALIZED VIEW test.view_table Engine = Memory AS SELECT * FROM test.src;

INSERT INTO test.src VALUES (1), (2), (3);
SELECT * FROM test.view_table ORDER BY x;

--Check if we can rename the view and if we can still fetch datas

RENAME TABLE test.view_table TO test.new_view_table;
SELECT * FROM test.new_view_table ORDER BY x;

DROP TABLE test.src;
DROP TABLE IF EXISTS test.view_table;
DROP TABLE IF EXISTS test.new_view_table;
