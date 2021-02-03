DROP TABLE IF EXISTS pk_order;

SET optimize_aggregation_in_order = 1;

CREATE TABLE pk_order(a UInt64, b UInt64, c UInt64, d UInt64) ENGINE=MergeTree() ORDER BY (a, b);
INSERT INTO pk_order(a, b, c, d) VALUES (1, 1, 101, 1), (1, 2, 102, 1), (1, 3, 103, 1), (1, 4, 104, 1);
INSERT INTO pk_order(a, b, c, d) VALUES (1, 5, 104, 1), (1, 6, 105, 1), (2, 1, 106, 2), (2, 1, 107, 2);
INSERT INTO pk_order(a, b, c, d) VALUES (2, 2, 107, 2), (2, 3, 108, 2), (2, 4, 109, 2);

-- Order after group by in order is determined

SELECT a, b FROM pk_order GROUP BY a, b;
SELECT a FROM pk_order GROUP BY a;

SELECT a, b, sum(c), avg(d) FROM pk_order GROUP BY a, b;
SELECT a, sum(c), avg(d) FROM pk_order GROUP BY a;
SELECT a, sum(c), avg(d) FROM pk_order GROUP BY -a;

DROP TABLE IF EXISTS pk_order;

CREATE TABLE pk_order (d DateTime, a Int32, b Int32) ENGINE = MergeTree ORDER BY (d, a)
    PARTITION BY toDate(d) SETTINGS index_granularity=1;

INSERT INTO pk_order
    SELECT toDateTime('2019-05-05 00:00:00') + INTERVAL number % 10 DAY, number, intHash32(number) from numbers(100);

set max_block_size = 1;

SELECT d, max(b) FROM pk_order GROUP BY d, a LIMIT 5;
SELECT d, avg(a) FROM pk_order GROUP BY toString(d) LIMIT 5;
SELECT toStartOfHour(d) as d1, min(a), max(b) FROM pk_order GROUP BY d1 LIMIT 5;

DROP TABLE pk_order;
