DROP TABLE IF EXISTS ttt01746;
CREATE TABLE ttt01746 (d Date, n UInt64) ENGINE = MergeTree() PARTITION BY toMonday(d) ORDER BY n SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
INSERT INTO ttt01746 SELECT toDate('2021-02-14') + (number % 30) AS d, number AS n FROM numbers(1500000);
set optimize_move_to_prewhere=0;
SELECT arraySort(x -> x.2, [tuple('a', 10)]) AS X FROM ttt01746 WHERE d >= toDate('2021-03-03') - 2 ORDER BY n LIMIT 1;
SELECT arraySort(x -> x.2, [tuple('a', 10)]) AS X FROM ttt01746 PREWHERE d >= toDate('2021-03-03') - 2 ORDER BY n LIMIT 1;
DROP TABLE ttt01746;
