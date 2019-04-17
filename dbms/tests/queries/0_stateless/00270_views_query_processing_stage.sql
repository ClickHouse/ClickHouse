DROP TABLE IF EXISTS view1;
DROP TABLE IF EXISTS view2;
DROP TABLE IF EXISTS merge_view;

CREATE VIEW view1 AS SELECT number FROM system.numbers LIMIT 10;
CREATE VIEW view2 AS SELECT number FROM system.numbers LIMIT 10;
CREATE TABLE merge_view (number UInt64) ENGINE = Merge(test, '^view');

SELECT 'Hello, world!' FROM merge_view LIMIT 5;

DROP TABLE view1;
DROP TABLE view2;
DROP TABLE merge_view;
