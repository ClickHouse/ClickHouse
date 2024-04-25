SET optimize_move_to_prewhere = 1;
SET enable_multiple_prewhere_read_steps = 1;

DROP TABLE IF EXISTS t_02848_mt1;
DROP TABLE IF EXISTS t_02848_mt2;

CREATE TABLE t_02848_mt1 (k UInt32, v String) ENGINE = MergeTree ORDER BY k SETTINGS min_bytes_for_wide_part=0;

INSERT INTO t_02848_mt1 SELECT number, toString(number) FROM numbers(100);

EXPLAIN SYNTAX SELECT count() FROM t_02848_mt1 WHERE k = 3 AND notEmpty(v);
SELECT count() FROM t_02848_mt1 WHERE k = 3 AND notEmpty(v);

CREATE TABLE t_02848_mt2 (a UInt32, b String, c Int32, d String) ENGINE = MergeTree ORDER BY (a,b,c) SETTINGS min_bytes_for_wide_part=0;

INSERT INTO t_02848_mt2 SELECT number, toString(number), number, 'aaaabbbbccccddddtestxxxyyy' FROM numbers(100);

-- the estimated column sizes are: {a: 428, b: 318, c: 428, d: 73}
-- it's not correct but let's fix it in the future.

EXPLAIN SYNTAX SELECT count() FROM t_02848_mt2 WHERE a = 3 AND b == '3' AND c < 20 AND d like '%es%';
SELECT count() FROM t_02848_mt2 WHERE a = 3 AND b == '3' AND c < 20 AND d like '%es%';

EXPLAIN SYNTAX SELECT count() FROM t_02848_mt2 WHERE a = 3 AND c < 20 AND c > 0 AND d like '%es%';
SELECT count() FROM t_02848_mt2 WHERE a = 3 AND c < 20 AND c > 0 AND d like '%es%';

EXPLAIN SYNTAX SELECT count() FROM t_02848_mt2 WHERE  b == '3' AND c < 20 AND d like '%es%';
SELECT count() FROM t_02848_mt2 WHERE b == '3' AND c < 20 AND d like '%es%';

EXPLAIN SYNTAX SELECT count() FROM t_02848_mt2 WHERE a = 3 AND b == '3' AND d like '%es%';
SELECT count() FROM t_02848_mt2 WHERE a = 3 AND b == '3' AND d like '%es%';

DROP TABLE t_02848_mt1;
DROP TABLE t_02848_mt2;
