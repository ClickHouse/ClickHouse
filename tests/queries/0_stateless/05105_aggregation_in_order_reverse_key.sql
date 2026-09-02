-- https://github.com/ClickHouse/ClickHouse/issues/111901
-- The aggregation-in-order analysis derived the sort description from the sorting key column names
-- only, assuming every column ascending. With a descending sorting key column the description said
-- the value increases along the table order while it decreases, so the transform merged rows into one
-- group per ascending prefix and `GROUP BY` silently returned fewer rows.

DROP TABLE IF EXISTS t_aio_reverse;
CREATE TABLE t_aio_reverse (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY (a, b DESC);
INSERT INTO t_aio_reverse SELECT number % 4, number FROM numbers(20);
INSERT INTO t_aio_reverse SELECT number % 4, number + 7 FROM numbers(20);

SELECT count() FROM (SELECT a, b FROM t_aio_reverse GROUP BY a, b);
SELECT count() FROM (SELECT a, b FROM t_aio_reverse GROUP BY a, b) SETTINGS optimize_aggregation_in_order = 1;
SELECT count() FROM (SELECT a, b FROM t_aio_reverse GROUP BY a, b) SETTINGS optimize_aggregation_in_order = 0;
SELECT a, b FROM t_aio_reverse GROUP BY a, b ORDER BY a, b LIMIT 5 SETTINGS optimize_aggregation_in_order = 1;
SELECT a, count(), sum(b) FROM t_aio_reverse GROUP BY a ORDER BY a SETTINGS optimize_aggregation_in_order = 1;
SELECT a, count(), sum(b) FROM t_aio_reverse GROUP BY a ORDER BY a SETTINGS optimize_aggregation_in_order = 0;

OPTIMIZE TABLE t_aio_reverse FINAL;
SELECT count() FROM (SELECT a, b FROM t_aio_reverse GROUP BY a, b) SETTINGS optimize_aggregation_in_order = 1;
SELECT count() FROM (SELECT a, b FROM t_aio_reverse GROUP BY a, b) SETTINGS optimize_aggregation_in_order = 0;
DROP TABLE t_aio_reverse;

SELECT 'an all-ascending key is unaffected';
DROP TABLE IF EXISTS t_aio_asc;
CREATE TABLE t_aio_asc (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY (a, b);
INSERT INTO t_aio_asc SELECT number % 4, number FROM numbers(20);
INSERT INTO t_aio_asc SELECT number % 4, number + 7 FROM numbers(20);
SELECT count() FROM (SELECT a, b FROM t_aio_asc GROUP BY a, b) SETTINGS optimize_aggregation_in_order = 1;
SELECT count() FROM (SELECT a, b FROM t_aio_asc GROUP BY a, b) SETTINGS optimize_aggregation_in_order = 0;
SELECT a, count() FROM t_aio_asc GROUP BY a ORDER BY a SETTINGS optimize_aggregation_in_order = 1;
DROP TABLE t_aio_asc;

SELECT 'a fully descending key';
DROP TABLE IF EXISTS t_aio_desc;
CREATE TABLE t_aio_desc (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY (a DESC, b DESC);
INSERT INTO t_aio_desc SELECT number % 4, number FROM numbers(20);
INSERT INTO t_aio_desc SELECT number % 4, number + 7 FROM numbers(20);
SELECT count() FROM (SELECT a, b FROM t_aio_desc GROUP BY a, b) SETTINGS optimize_aggregation_in_order = 1;
SELECT count() FROM (SELECT a, b FROM t_aio_desc GROUP BY a, b) SETTINGS optimize_aggregation_in_order = 0;
SELECT a, count() FROM t_aio_desc GROUP BY a ORDER BY a SETTINGS optimize_aggregation_in_order = 1;
DROP TABLE t_aio_desc;

SELECT 'the optimization still engages, with the direction of each key column';
DROP TABLE IF EXISTS t_aio_explain;
CREATE TABLE t_aio_explain (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY (a, b DESC);
INSERT INTO t_aio_explain SELECT number % 4, number FROM numbers(40);
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT a, b, count() FROM t_aio_explain GROUP BY a, b
    SETTINGS optimize_aggregation_in_order = 1) WHERE explain LIKE '%Order:%';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT a, count() FROM t_aio_explain GROUP BY a
    SETTINGS optimize_aggregation_in_order = 1) WHERE explain LIKE '%Order:%';
DROP TABLE t_aio_explain;

SELECT 'reading in order over the same key stays correct';
DROP TABLE IF EXISTS t_aio_read;
CREATE TABLE t_aio_read (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY (a, b DESC);
INSERT INTO t_aio_read SELECT number % 4, number FROM numbers(20);
SELECT a, b FROM t_aio_read ORDER BY a, b DESC LIMIT 3 SETTINGS optimize_read_in_order = 1;
SELECT count() FROM (SELECT DISTINCT a, b FROM t_aio_read) SETTINGS optimize_distinct_in_order = 1;
DROP TABLE t_aio_read;
