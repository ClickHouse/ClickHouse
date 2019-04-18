DROP TABLE IF EXISTS test.view1;
DROP TABLE IF EXISTS test.view2;
DROP TABLE IF EXISTS test.merge_view;

CREATE VIEW test.view1 AS SELECT number FROM system.numbers LIMIT 10;
CREATE VIEW test.view2 AS SELECT number FROM system.numbers LIMIT 10;
CREATE TABLE test.merge_view (number UInt64) ENGINE = Merge(test, '^view');

SELECT 'Hello, world!' FROM test.merge_view LIMIT 5;

DROP TABLE test.view1;
DROP TABLE test.view2;
DROP TABLE test.merge_view;
