DROP TABLE IF EXISTS test.test;
DROP TABLE IF EXISTS test.test_view;

CREATE TABLE test.test(id UInt64) ENGINE = Log;
CREATE VIEW test.test_view AS SELECT * FROM test.test WHERE id = (SELECT 1);

DETACH TABLE test.test_view;
ATTACH TABLE test.test_view;

SHOW CREATE TABLE test.test_view;

DROP TABLE IF EXISTS test.test;
DROP TABLE IF EXISTS test.test_view;
