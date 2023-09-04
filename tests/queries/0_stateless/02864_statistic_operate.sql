DROP TABLE IF EXISTS t1;

SET allow_experimental_statistic = 1;
SET allow_statistic_optimize = 1;

CREATE TABLE t1 
(
    a Int64,
    b Float64,
    pk String,
    STATISTIC a TYPE tdigest,
    STATISTIC b TYPE tdigest
) Engine = MergeTree() ORDER BY pk;

SHOW CREATE TABLE t1;

INSERT INTO t1 select number, -number, generateUUIDv4() FROM system.numbers LIMIT 10000;

SELECT 'After insert';
EXPLAIN SYNTAX SELECT count(*) FROM t1 WHERE b < 10 and a < 10;
SELECT count(*) FROM t1 WHERE b < 10 and a < 10;

ALTER TABLE t1 DROP STATISTIC a TYPE tdigest;
ALTER TABLE t1 DROP STATISTIC b TYPE tdigest;

SELECT 'After drop statistic';
EXPLAIN SYNTAX SELECT count(*) FROM t1 WHERE b < 10 and a < 10;
SELECT count(*) FROM t1 WHERE b < 10 and a < 10;

ALTER TABLE t1 ADD STATISTIC a TYPE tdigest;
ALTER TABLE t1 ADD STATISTIC b TYPE tdigest;

ALTER TABLE t1 MATERIALIZE STATISTIC a TYPE tdigest;
ALTER TABLE t1 MATERIALIZE STATISTIC b TYPE tdigest;
INSERT INTO t1 select number, -number, generateUUIDv4() FROM system.numbers LIMIT 10000;

SELECT 'After materialize statistic';
EXPLAIN SYNTAX SELECT count(*) FROM t1 WHERE b < 10 and a < 10;
SELECT count(*) FROM t1 WHERE b < 10 and a < 10;

OPTIMIZE TABLE t1 FINAL;

SELECT 'After merge';
EXPLAIN SYNTAX SELECT count(*) FROM t1 WHERE b < 10 and a < 10;
SELECT count(*) FROM t1 WHERE b < 10 and a < 10;

DROP TABLE IF EXISTS t1;
