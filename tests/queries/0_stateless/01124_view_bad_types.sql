DROP TABLE IF EXISTS test.table;
CREATE TABLE test.table (x UInt16) ENGINE = TinyLog;
INSERT INTO test.table SELECT * FROM system.numbers LIMIT 10;

DROP TABLE IF EXISTS test.view;
CREATE VIEW test.view (x UInt64) AS SELECT * FROM test.table;

SELECT x, any(x) FROM test.view GROUP BY x ORDER BY x;

DROP TABLE test.view;
DROP TABLE test.table;
