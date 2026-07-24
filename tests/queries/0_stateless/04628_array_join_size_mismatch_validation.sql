-- Tags: no-random-settings
-- Regression test for issue #111747: count()/projection over a multi-array ARRAY JOIN with
-- mismatched per-row array sizes must throw SIZES_OF_ARRAYS_DONT_MATCH, exactly like SELECT *.
-- Previously the unused sibling arrays were pruned before execution, so the size check was skipped
-- and the query silently returned a result that matched no valid materialization.

DROP TABLE IF EXISTS t_mismatch;
CREATE TABLE t_mismatch (id UInt32, a Array(String), b Array(String), c Array(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_mismatch VALUES (1, ['x', 'y'], ['p'], ['m', 'n']);

SELECT 'Mismatched sizes: every projection must throw, matching SELECT *';
SELECT count() FROM t_mismatch ARRAY JOIN a, b; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
SELECT a FROM t_mismatch ARRAY JOIN a, b; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
SELECT id FROM t_mismatch ARRAY JOIN a, b; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
SELECT count() FROM t_mismatch ARRAY JOIN a, b, c; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
SELECT count() FROM t_mismatch LEFT ARRAY JOIN a, b; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
SELECT * FROM t_mismatch ARRAY JOIN a, b; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }

DROP TABLE t_mismatch;

DROP TABLE IF EXISTS t_match;
CREATE TABLE t_match (id UInt32, a Array(String), b Array(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_match VALUES (1, ['x', 'y'], ['p', 'q']), (2, ['z'], ['r']);

SELECT 'Matching sizes: count() and projection are unchanged';
SELECT count() FROM t_match ARRAY JOIN a, b;
SELECT a FROM t_match ARRAY JOIN a, b ORDER BY a;
SELECT count() FROM t_match LEFT ARRAY JOIN a, b;

DROP TABLE t_match;

DROP TABLE IF EXISTS t_single;
CREATE TABLE t_single (id UInt32, a Array(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_single VALUES (1, ['x', 'y', 'z']);

SELECT 'Single-array ARRAY JOIN is unaffected';
SELECT count() FROM t_single ARRAY JOIN a;
SELECT a FROM t_single ARRAY JOIN a ORDER BY a;

DROP TABLE t_single;
