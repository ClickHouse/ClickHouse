DROP TABLE if exists lc;
CREATE TABLE lc (a LowCardinality(Nullable(String)), b LowCardinality(Nullable(String))) ENGINE = MergeTree order by tuple();
INSERT INTO lc VALUES ('a', 'b');
INSERT INTO lc VALUES ('c', 'd');

SELECT a, b, count(a) FROM lc GROUP BY a, b WITH ROLLUP ORDER BY a, b;
SELECT a, count(a) FROM lc GROUP BY a WITH ROLLUP ORDER BY a;

SELECT a, b, count(a) FROM lc GROUP BY a, b WITH CUBE ORDER BY a, b;
SELECT a, count(a) FROM lc GROUP BY a WITH CUBE ORDER BY a;

DROP TABLE if exists lc;
