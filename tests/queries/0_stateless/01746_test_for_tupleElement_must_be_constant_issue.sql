DROP TABLE IF EXISTS ttt01746;
CREATE TABLE ttt01746 (d Date, n UInt64) ENGINE = MergeTree() PARTITION BY toMonday(d) ORDER BY n;
INSERT INTO ttt01746 SELECT toDate('2021-02-14') + (number % 30) AS d, number AS n FROM numbers(100000);
SELECT arraySort(x -> x.2, [tuple('a', 10)]) AS X FROM ttt01746 WHERE d >= toDate('2021-03-03') - 2 ORDER BY n LIMIT 1;
DROP TABLE ttt01746;
