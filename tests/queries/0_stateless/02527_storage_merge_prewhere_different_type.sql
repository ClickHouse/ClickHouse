DROP TABLE IF EXISTS foo;
DROP TABLE IF EXISTS merge1;
DROP TABLE IF EXISTS merge2;

CREATE TABLE foo(Id Int32, Val Nullable(Int32)) Engine=MergeTree ORDER BY Id;
INSERT INTO foo VALUES (1, 2), (3, 4);

CREATE TABLE merge1(Id Int32, Val Int32) Engine=Merge(currentDatabase(), '^foo');
SELECT Val FROM merge1 PREWHERE Val = 65536 OR Val = 2; -- { serverError ILLEGAL_PREWHERE }

CREATE TABLE merge2(Id Int32, Val Nullable(Int32)) Engine=Merge(currentDatabase(), '^foo');
SELECT Val FROM merge2 PREWHERE Val = 65536 OR Val = 2;

DROP TABLE merge2;
DROP TABLE merge1;
DROP TABLE foo;
