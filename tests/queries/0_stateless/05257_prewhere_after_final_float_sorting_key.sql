-- -0.0 and 0.0 dedup as one key, and so do all NaN payloads, so a condition over a floating-point
-- sorting key column must not be filtered before FINAL: it can drop the row that wins the merge
SET explain_query_plan_default = 'legacy';
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;
SET optimize_move_to_prewhere_if_final = 1;

DROP TABLE IF EXISTS t_prewhere_final_float;

CREATE TABLE t_prewhere_final_float (id UInt64, f Float64, v UInt64, s String)
ENGINE = ReplacingMergeTree(v) ORDER BY (id, f);

-- merges off and one row per insert, so the superseded version really is stored on its own
SYSTEM STOP MERGES t_prewhere_final_float;
INSERT INTO t_prewhere_final_float VALUES (1, 0.0, 1, 'stale');
INSERT INTO t_prewhere_final_float VALUES (1, -0.0, 2, 'winner');

SELECT '= both versions are stored =';
SELECT count() FROM t_prewhere_final_float;

SELECT '= FINAL keeps the newer version =';
SELECT id, f, v, s FROM t_prewhere_final_float FINAL;

SELECT '= a condition over the float key column is not moved =';
SELECT count() FROM (EXPLAIN actions=1 SELECT id FROM t_prewhere_final_float FINAL WHERE toString(f) = '0') WHERE explain LIKE '%Prewhere filter%';

-- the condition matches only the superseded version, so the answer is empty
SELECT '= the superseded version does not come back =';
SELECT id, f, v, s FROM t_prewhere_final_float FINAL WHERE toString(f) = '0';

SELECT '= nothing moves when the optimization is off =';
SELECT count() FROM (EXPLAIN actions=1 SELECT id FROM t_prewhere_final_float FINAL WHERE toString(f) = '0' SETTINGS optimize_move_to_prewhere_if_final = 0) WHERE explain LIKE '%Prewhere filter%';
SELECT id, f, v, s FROM t_prewhere_final_float FINAL WHERE toString(f) = '0' SETTINGS optimize_move_to_prewhere_if_final = 0;

DROP TABLE t_prewhere_final_float;

SELECT '= only the float column of a mixed sorting key is held back =';
DROP TABLE IF EXISTS t_prewhere_final_mixed;
CREATE TABLE t_prewhere_final_mixed (id UInt64, f Float64, g UInt64, v UInt64, s String)
ENGINE = ReplacingMergeTree(v) ORDER BY (id, f, g);
SYSTEM STOP MERGES t_prewhere_final_mixed;
INSERT INTO t_prewhere_final_mixed VALUES (1, 0.0, 7, 1, 'stale');
INSERT INTO t_prewhere_final_mixed VALUES (1, -0.0, 7, 2, 'winner');
SELECT count() FROM (EXPLAIN actions=1 SELECT id FROM t_prewhere_final_mixed FINAL WHERE toString(f) = '0') WHERE explain LIKE '%Prewhere filter%';
SELECT count() > 0 FROM (EXPLAIN actions=1 SELECT id FROM t_prewhere_final_mixed FINAL WHERE toString(g) = '7') WHERE explain LIKE '%Prewhere filter%';
SELECT id, f, g, v, s FROM t_prewhere_final_mixed FINAL WHERE toString(g) = '7';
DROP TABLE t_prewhere_final_mixed;

SELECT '= NaN payloads dedup as one key =';
DROP TABLE IF EXISTS t_prewhere_final_nan;
CREATE TABLE t_prewhere_final_nan (id UInt64, f Float64, v UInt64) ENGINE = ReplacingMergeTree(v) ORDER BY (id, f);
SYSTEM STOP MERGES t_prewhere_final_nan;
INSERT INTO t_prewhere_final_nan VALUES (1, reinterpretAsFloat64(toUInt64(9221120237041090560)), 1);
INSERT INTO t_prewhere_final_nan VALUES (1, reinterpretAsFloat64(toUInt64(9221120237041090561)), 2);
SELECT count() FROM (EXPLAIN actions=1 SELECT id FROM t_prewhere_final_nan FINAL WHERE reinterpretAsUInt64(f) = 9221120237041090560) WHERE explain LIKE '%Prewhere filter%';
SELECT id, reinterpretAsUInt64(f), v FROM t_prewhere_final_nan FINAL WHERE reinterpretAsUInt64(f) = 9221120237041090560;
DROP TABLE t_prewhere_final_nan;

SELECT '= nullable float sorting key =';
DROP TABLE IF EXISTS t_prewhere_final_null;
CREATE TABLE t_prewhere_final_null (id UInt64, f Nullable(Float64), v UInt64)
ENGINE = ReplacingMergeTree(v) ORDER BY (id, f) SETTINGS allow_nullable_key = 1;
SYSTEM STOP MERGES t_prewhere_final_null;
INSERT INTO t_prewhere_final_null VALUES (1, 0.0, 1);
INSERT INTO t_prewhere_final_null VALUES (1, -0.0, 2);
SELECT count() FROM (EXPLAIN actions=1 SELECT id FROM t_prewhere_final_null FINAL WHERE toString(f) = '0') WHERE explain LIKE '%Prewhere filter%';
SELECT id, f, v FROM t_prewhere_final_null FINAL WHERE toString(f) = '0';
DROP TABLE t_prewhere_final_null;

SELECT '= tuple with a float inside the sorting key =';
DROP TABLE IF EXISTS t_prewhere_final_tuple;
CREATE TABLE t_prewhere_final_tuple (id UInt64, t Tuple(Float64, UInt8), v UInt64)
ENGINE = ReplacingMergeTree(v) ORDER BY (id, t);
SYSTEM STOP MERGES t_prewhere_final_tuple;
INSERT INTO t_prewhere_final_tuple VALUES (1, (0.0, 1), 1);
INSERT INTO t_prewhere_final_tuple VALUES (1, (-0.0, 1), 2);
SELECT count() FROM (EXPLAIN actions=1 SELECT id FROM t_prewhere_final_tuple FINAL WHERE toString(t) = '(0,1)') WHERE explain LIKE '%Prewhere filter%';
SELECT id, t, v FROM t_prewhere_final_tuple FINAL WHERE toString(t) = '(0,1)';
DROP TABLE t_prewhere_final_tuple;

SELECT '= float nested two levels deep in the sorting key =';
DROP TABLE IF EXISTS t_prewhere_final_array;
CREATE TABLE t_prewhere_final_array (id UInt64, a Array(Tuple(Float64, UInt8)), v UInt64)
ENGINE = ReplacingMergeTree(v) ORDER BY (id, a);
SYSTEM STOP MERGES t_prewhere_final_array;
INSERT INTO t_prewhere_final_array VALUES (1, [(0.0, 1)], 1);
INSERT INTO t_prewhere_final_array VALUES (1, [(-0.0, 1)], 2);
SELECT count() FROM (EXPLAIN actions=1 SELECT id FROM t_prewhere_final_array FINAL WHERE toString(a) = '[(0,1)]') WHERE explain LIKE '%Prewhere filter%';
SELECT id, a, v FROM t_prewhere_final_array FINAL WHERE toString(a) = '[(0,1)]';
DROP TABLE t_prewhere_final_array;

SELECT '= an integer sorting key still moves =';
DROP TABLE IF EXISTS t_prewhere_final_int;
CREATE TABLE t_prewhere_final_int (id UInt64, u UInt64, v UInt64, s String)
ENGINE = ReplacingMergeTree(v) ORDER BY (id, u);
SYSTEM STOP MERGES t_prewhere_final_int;
INSERT INTO t_prewhere_final_int VALUES (1, 5, 1, 'stale');
INSERT INTO t_prewhere_final_int VALUES (1, 5, 2, 'winner');
SELECT count() > 0 FROM (EXPLAIN actions=1 SELECT id FROM t_prewhere_final_int FINAL WHERE toString(u) = '5') WHERE explain LIKE '%Prewhere filter%';
SELECT id, u, v, s FROM t_prewhere_final_int FINAL WHERE toString(u) = '5';
DROP TABLE t_prewhere_final_int;
