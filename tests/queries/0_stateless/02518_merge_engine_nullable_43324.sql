
DROP TABLE IF EXISTS foo;
DROP TABLE IF EXISTS foo__fuzz_0;
DROP TABLE IF EXISTS foo_merge;

CREATE TABLE foo (`Id` Int32, `Val` Int32) ENGINE = MergeTree ORDER BY Id;
CREATE TABLE foo__fuzz_0 (`Id` Int64, `Val` Nullable(Int32)) ENGINE = MergeTree ORDER BY Id;

INSERT INTO foo SELECT number, number % 5 FROM numbers(10);
INSERT INTO foo__fuzz_0 SELECT number, number % 5 FROM numbers(10);

CREATE TABLE merge1 AS foo ENGINE = Merge(currentDatabase(), '^foo');
CREATE TABLE merge2 (`Id` Int32, `Val` Int32) ENGINE = Merge(currentDatabase(), '^foo');
CREATE TABLE merge3 (`Id` Int32, `Val` Int32) ENGINE = Merge(currentDatabase(), '^foo__fuzz_0');

SELECT * FROM merge1 WHERE Val = 3 AND Val = 1;
SELECT * FROM merge2 WHERE Val = 3 AND Val = 1;
SELECT * FROM merge3 WHERE Val = 3 AND Val = 1;
