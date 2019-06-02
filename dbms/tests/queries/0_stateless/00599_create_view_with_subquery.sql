CREATE TABLE test(id UInt64) ENGINE = Log;
CREATE VIEW test_view AS SELECT * FROM test WHERE id = (SELECT 1);

DETACH TABLE test_view;
ATTACH TABLE test_view;

SHOW CREATE TABLE test_view;
