DROP TABLE if exists lc;
CREATE TABLE lc (a LowCardinality(Nullable(String)), b LowCardinality(Nullable(String))) ENGINE = MergeTree order by tuple();
INSERT INTO lc VALUES ('a', 'b');
INSERT INTO lc VALUES ('c', 'd');

SELECT a, b, count(a) FROM lc GROUP BY a, b WITH ROLLUP;
SELECT a, count(a) FROM lc GROUP BY a WITH ROLLUP;

SELECT a, b, count(a) FROM lc GROUP BY a, b WITH CUBE;
SELECT a, count(a) FROM lc GROUP BY a WITH CUBE;

DROP TABLE if exists lc;
