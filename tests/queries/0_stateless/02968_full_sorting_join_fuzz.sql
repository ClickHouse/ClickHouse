SET max_bytes_in_join = 0, join_algorithm = 'full_sorting_merge', max_block_size = 10240;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (`key` UInt32, `s` String) ENGINE = MergeTree ORDER BY key;
CREATE TABLE t2 (`key` UInt32, `s` String) ENGINE = MergeTree ORDER BY key;

INSERT INTO t1 SELECT (sipHash64(number, 'x') % 10000000) + 1 AS key, concat('val', toString(number)) AS s FROM numbers_mt(10000000);
INSERT INTO t2 SELECT (sipHash64(number, 'y') % 1000000) + 1 AS key, concat('val', toString(number)) AS s FROM numbers_mt(1000000);

SELECT materialize([NULL]), [], 100, count(materialize(NULL)) FROM t1 ALL INNER JOIN t2 ON t1.key = t2.key PREWHERE 10 WHERE t2.key != 0 WITH TOTALS;

DROP TABLE t1;
DROP TABLE t2;
