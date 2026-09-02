DROP TABLE IF EXISTS foo;
DROP TABLE IF EXISTS foo_merge;
DROP TABLE IF EXISTS t2;

CREATE TABLE foo(Id Int32, Val Int32) Engine=MergeTree PARTITION BY Val ORDER BY Id;
INSERT INTO foo SELECT number, number%5 FROM numbers(100000);

CREATE TABLE foo_merge as foo ENGINE=Merge(currentDatabase(), '^foo');

CREATE TABLE t2 (Id Int32, Val Int32, X Int32) Engine=Memory;
INSERT INTO t2 values (4, 3, 4);

SET force_primary_key = 1, force_index_by_date=1;

SELECT * FROM foo_merge WHERE Val = 3 AND Id = 3;
SELECT count(), X FROM foo_merge JOIN t2 USING Val WHERE Val = 3 AND Id = 3 AND t2.X == 4 GROUP BY X;
SELECT count(), X FROM foo_merge JOIN t2 USING Val WHERE Val = 3 AND (Id = 3 AND t2.X == 4) GROUP BY X;
SELECT count(), X FROM foo_merge JOIN t2 USING Val WHERE Val = 3 AND Id = 3 GROUP BY X;
SELECT count(), X FROM (SELECT * FROM foo_merge) f JOIN t2 USING Val WHERE Val = 3 AND Id = 3 GROUP BY X;

DROP TABLE IF EXISTS foo;
DROP TABLE IF EXISTS foo_merge;
DROP TABLE IF EXISTS t2;
