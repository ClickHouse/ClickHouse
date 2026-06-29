SET enable_analyzer = 1;

DROP TABLE IF EXISTS t;
CREATE TABLE t (id Int32, d Tuple(year Int32, month Int32, day Int32))
ENGINE = MergeTree ORDER BY ();

ALTER TABLE t ADD COLUMN dt Date MATERIALIZED makeDate(d.year, d.month, d.day);
INSERT INTO t (id, d) SELECT number, (2000 + number, 1 + number % 10, 1 + number % 30) FROM numbers(10);

SELECT *, dt FROM t;
DROP TABLE t;
