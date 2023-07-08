DROP TABLE IF EXISTS foo;
DROP TABLE IF EXISTS merge1;

CREATE TABLE foo (`Id` Int32, `Val` Int32) ENGINE = MergeTree ORDER BY Id;
INSERT INTO foo SELECT number, number FROM numbers(100);

CREATE TABLE merge1 AS foo ENGINE = Merge(currentDatabase(), '^foo');

EXPLAIN PIPELINE graph = 1, compact = 1 SELECT * FROM merge1 FORMAT Null;
