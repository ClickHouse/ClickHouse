DROP TABLE IF EXISTS t_read_in_order;

CREATE TABLE t_read_in_order(date Date, i UInt64, v UInt64)
ENGINE = MergeTree ORDER BY (date, i);

INSERT INTO t_read_in_order SELECT '2020-10-10', number % 10, number FROM numbers(100000);
INSERT INTO t_read_in_order SELECT '2020-10-11', number % 10, number FROM numbers(100000);

SELECT date, i FROM t_read_in_order WHERE date = '2020-10-11' ORDER BY i LIMIT 10;
EXPLAIN PIPELINE SELECT date, i FROM t_read_in_order WHERE date = '2020-10-11' ORDER BY i LIMIT 10;

SELECT * FROM t_read_in_order WHERE date = '2020-10-11' ORDER BY i, v LIMIT 10;
EXPLAIN PIPELINE SELECT * FROM t_read_in_order WHERE date = '2020-10-11' ORDER BY i, v LIMIT 10;

INSERT INTO t_read_in_order SELECT '2020-10-12', number, number FROM numbers(100000);
SELECT date, i FROM t_read_in_order WHERE date = '2020-10-12' ORDER BY i LIMIT 10;

EXPLAIN PIPELINE SELECT date, i FROM t_read_in_order WHERE date = '2020-10-12' ORDER BY i DESC LIMIT 10;
SELECT date, i FROM t_read_in_order WHERE date = '2020-10-12' ORDER BY i DESC LIMIT 10;

DROP TABLE t_read_in_order;
