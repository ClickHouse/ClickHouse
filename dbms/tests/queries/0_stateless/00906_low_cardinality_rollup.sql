DROP TABLE if exists test.lc;
CREATE TABLE test.lc (a LowCardinality(Nullable(String)), b LowCardinality(Nullable(String))) ENGINE = MergeTree order by tuple();
INSERT INTO test.lc VALUES ('a', 'b');
INSERT INTO test.lc VALUES ('c', 'd');

SELECT a, b, count(a) FROM test.lc GROUP BY a, b WITH ROLLUP;
SELECT a, count(a) FROM test.lc GROUP BY a WITH ROLLUP;

SELECT a, b, count(a) FROM test.lc GROUP BY a, b WITH CUBE;
SELECT a, count(a) FROM test.lc GROUP BY a WITH CUBE;

DROP TABLE if exists test.lc;
