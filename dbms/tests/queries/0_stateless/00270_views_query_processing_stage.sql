CREATE VIEW view1 AS SELECT number FROM system.numbers LIMIT 10;
CREATE VIEW view2 AS SELECT number FROM system.numbers LIMIT 10;
CREATE TABLE merge_view (number UInt64) ENGINE = Merge(default, '^view');

SELECT 'Hello, world!' FROM merge_view LIMIT 5;
