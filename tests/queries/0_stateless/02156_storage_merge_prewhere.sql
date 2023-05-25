SET optimize_move_to_prewhere = 1;

DROP TABLE IF EXISTS t_02156_mt1;
DROP TABLE IF EXISTS t_02156_mt2;
DROP TABLE IF EXISTS t_02156_log;
DROP TABLE IF EXISTS t_02156_dist;
DROP TABLE IF EXISTS t_02156_merge1;
DROP TABLE IF EXISTS t_02156_merge2;
DROP TABLE IF EXISTS t_02156_merge3;

CREATE TABLE t_02156_mt1 (k UInt32, v String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_02156_mt2 (k UInt32, v String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_02156_log (k UInt32, v String) ENGINE = Log;

CREATE TABLE t_02156_dist (k UInt32, v String) ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_02156_mt1);

CREATE TABLE t_02156_merge1 (k UInt32, v String) ENGINE = Merge(currentDatabase(), 't_02156_mt1|t_02156_mt2');
CREATE TABLE t_02156_merge2 (k UInt32, v String) ENGINE = Merge(currentDatabase(), 't_02156_mt1|t_02156_log');
CREATE TABLE t_02156_merge3 (k UInt32, v String) ENGINE = Merge(currentDatabase(), 't_02156_mt2|t_02156_dist');

INSERT INTO t_02156_mt1 SELECT number, toString(number) FROM numbers(10000);
INSERT INTO t_02156_mt2 SELECT number, toString(number) FROM numbers(10000);
INSERT INTO t_02156_log SELECT number, toString(number) FROM numbers(10000);

EXPLAIN SYNTAX SELECT count() FROM t_02156_merge1 WHERE k = 3 AND notEmpty(v);
SELECT count() FROM t_02156_merge1 WHERE k = 3 AND notEmpty(v);

EXPLAIN SYNTAX SELECT count() FROM t_02156_merge2 WHERE k = 3 AND notEmpty(v);
SELECT count() FROM t_02156_merge2 WHERE k = 3 AND notEmpty(v);

EXPLAIN SYNTAX SELECT count() FROM t_02156_merge3 WHERE k = 3 AND notEmpty(v);
SELECT count() FROM t_02156_merge3 WHERE k = 3 AND notEmpty(v);

DROP TABLE IF EXISTS t_02156_mt1;
DROP TABLE IF EXISTS t_02156_mt2;
DROP TABLE IF EXISTS t_02156_log;
DROP TABLE IF EXISTS t_02156_dist;
DROP TABLE IF EXISTS t_02156_merge1;
DROP TABLE IF EXISTS t_02156_merge2;
DROP TABLE IF EXISTS t_02156_merge3;
