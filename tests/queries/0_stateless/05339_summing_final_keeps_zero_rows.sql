-- `SELECT ... FINAL` from `SummingMergeTree` sums the rows with equal keys, but does not remove
-- the rows whose sums are all zero, unlike a background merge. The result does not depend on
-- the set of columns that the query reads.

DROP TABLE IF EXISTS t_summing_final_zero;

CREATE TABLE t_summing_final_zero (c0 Int8, c1 Int32, c3 UInt64)
ENGINE = SummingMergeTree ORDER BY c3;

SYSTEM STOP MERGES t_summing_final_zero;

INSERT INTO t_summing_final_zero SELECT 1, number, number FROM numbers(10);
INSERT INTO t_summing_final_zero SELECT -1, if(number < 5, -number, 0), number FROM numbers(10);

SELECT 'count';
SELECT count() FROM t_summing_final_zero FINAL;
SELECT 'subset of the summed columns';
SELECT c0, c3 FROM t_summing_final_zero FINAL ORDER BY c3;
SELECT 'all columns';
SELECT * FROM t_summing_final_zero FINAL ORDER BY c3;
SELECT 'sorting key only';
SELECT c3 FROM t_summing_final_zero FINAL ORDER BY c3;

SELECT 'after merge';
SYSTEM START MERGES t_summing_final_zero;
OPTIMIZE TABLE t_summing_final_zero FINAL;
SELECT * FROM t_summing_final_zero ORDER BY c3;
SELECT count() FROM t_summing_final_zero FINAL;

DROP TABLE t_summing_final_zero;
