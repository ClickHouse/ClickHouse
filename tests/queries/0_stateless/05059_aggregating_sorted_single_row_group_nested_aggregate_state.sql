-- SimpleAggregateFunction(any | anyLast, AggregateFunction(...)) keeps nested aggregate states in ColumnAggregateFunction.
-- A one-row group in AggregatingSortedAlgorithm is copied with insertFrom, which for this column shares the ownership
-- of the states with the source column instead of going through the add + insertResultInto round-trip.
-- Check that FINAL and merges produce the same states for one-row and multi-row groups, and that the states
-- can be merged further afterwards.

DROP TABLE IF EXISTS t_nested_states;

CREATE TABLE t_nested_states
(
    k UInt32,
    a SimpleAggregateFunction(any, AggregateFunction(uniq, UInt64)),
    al SimpleAggregateFunction(anyLast, AggregateFunction(uniq, UInt64)),
    arr SimpleAggregateFunction(any, Array(AggregateFunction(uniq, UInt64)))
)
ENGINE = AggregatingMergeTree ORDER BY k;

SYSTEM STOP MERGES t_nested_states;

-- Every state column gets its own expression on purpose: feeding two SimpleAggregateFunction(any, AggregateFunction(...))
-- columns from one shared `uniqState(v)` column trips a pre-existing bug in the merge on insert when the INSERT
-- SELECT delivers more than one block (for example with group_by_two_level_threshold = 1), which is not what this
-- test is about.
-- Part 1: keys 0..9, the state of key k counts the k + 1 values 0..k.
INSERT INTO t_nested_states
SELECT k, uniqState(v), uniqState(v + 0), [uniqState(v * 1), uniqState(toUInt64(v % 3))]
FROM (SELECT number AS k, arrayJoin(range(number + 1)) AS v FROM numbers(10))
GROUP BY k;

-- Part 2: keys 5..14, the state of key k counts the 2 * (k + 1) values 100..100 + 2 * k + 1.
INSERT INTO t_nested_states
SELECT k, uniqState(v), uniqState(v + 0), [uniqState(v * 1), uniqState(toUInt64(v % 5))]
FROM (SELECT number AS k, arrayJoin(range(100, 100 + 2 * (number + 1))) AS v FROM numbers(5, 10))
GROUP BY k;

SELECT 'final';
SELECT k, finalizeAggregation(a), finalizeAggregation(al), arrayMap(x -> finalizeAggregation(x), arr) FROM t_nested_states FINAL ORDER BY k;

SELECT 'final, small blocks';
SELECT k, finalizeAggregation(a), finalizeAggregation(al), arrayMap(x -> finalizeAggregation(x), arr) FROM t_nested_states FINAL ORDER BY k SETTINGS max_block_size = 4;

SELECT 'states merged over final';
SELECT uniqMerge(a), uniqMerge(al), uniqMerge(arr[1]), uniqMerge(arr[2]) FROM t_nested_states FINAL;
SELECT uniqMerge(a), uniqMerge(al), uniqMerge(arr[1]), uniqMerge(arr[2]) FROM t_nested_states FINAL SETTINGS max_block_size = 4;

SYSTEM START MERGES t_nested_states;
OPTIMIZE TABLE t_nested_states FINAL;

SELECT 'optimized';
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_nested_states' AND active;
SELECT k, finalizeAggregation(a), finalizeAggregation(al), arrayMap(x -> finalizeAggregation(x), arr) FROM t_nested_states ORDER BY k;

SELECT 'states merged after optimize';
SELECT uniqMerge(a), uniqMerge(al), uniqMerge(arr[1]), uniqMerge(arr[2]) FROM t_nested_states;

-- Part 3: every key again, the state counts the 3 values 200..202. Every group of the merged part now gets more rows.
INSERT INTO t_nested_states
SELECT k, uniqState(v), uniqState(v + 0), [uniqState(v * 1), uniqState(toUInt64(v % 7))]
FROM (SELECT number AS k, arrayJoin(range(toUInt64(200), 203)) AS v FROM numbers(15))
GROUP BY k;

SELECT 'final after the second insert';
SELECT k, finalizeAggregation(a), finalizeAggregation(al), arrayMap(x -> finalizeAggregation(x), arr) FROM t_nested_states FINAL ORDER BY k;

SELECT 'states merged over final after the second insert';
SELECT uniqMerge(a), uniqMerge(al), uniqMerge(arr[1]), uniqMerge(arr[2]) FROM t_nested_states FINAL;

OPTIMIZE TABLE t_nested_states FINAL;

SELECT 'optimized again';
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_nested_states' AND active;
SELECT k, finalizeAggregation(a), finalizeAggregation(al), arrayMap(x -> finalizeAggregation(x), arr) FROM t_nested_states ORDER BY k;

SELECT 'states merged after the second optimize';
SELECT uniqMerge(a), uniqMerge(al), uniqMerge(arr[1]), uniqMerge(arr[2]) FROM t_nested_states;

DROP TABLE t_nested_states;
