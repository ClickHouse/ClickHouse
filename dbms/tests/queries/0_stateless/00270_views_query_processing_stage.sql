DROP TABLE IF EXISTS view1_00270;
DROP TABLE IF EXISTS view2_00270;
DROP TABLE IF EXISTS merge_view_00270;

CREATE VIEW view1_00270 AS SELECT number FROM system.numbers LIMIT 10;
CREATE VIEW view2_00270 AS SELECT number FROM system.numbers LIMIT 10;
CREATE TABLE merge_view_00270 (number UInt64) ENGINE = Merge(currentDatabase(), '^view');

SELECT 'Hello, world!' FROM merge_view_00270 LIMIT 5;

DROP TABLE view1_00270;
DROP TABLE view2_00270;
DROP TABLE merge_view_00270;
