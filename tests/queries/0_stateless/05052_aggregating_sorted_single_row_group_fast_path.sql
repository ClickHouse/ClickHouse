-- One-row groups in AggregatingSortedAlgorithm are copied without going through the aggregate state.
-- Check that the result matches an explicit GROUP BY for identity functions and that functions
-- normalizing a single value still do so.

DROP TABLE IF EXISTS t_identity;

CREATE TABLE t_identity
(
    k UInt32,
    mn SimpleAggregateFunction(min, UInt32),
    mx SimpleAggregateFunction(max, UInt32),
    s SimpleAggregateFunction(sum, UInt64),
    ba SimpleAggregateFunction(groupBitAnd, UInt32),
    al SimpleAggregateFunction(anyLast, Nullable(UInt32)),
    a SimpleAggregateFunction(any, String),
    lc SimpleAggregateFunction(max, LowCardinality(String))
)
ENGINE = AggregatingMergeTree ORDER BY k;

INSERT INTO t_identity SELECT number, number, number, number, number, if(number % 3 = 0, NULL, number), toString(number), toString(number % 7) FROM numbers(20);
INSERT INTO t_identity SELECT number, number + 100, number + 100, number, number + 1, NULL, toString(number), 'z' FROM numbers(5, 3);
INSERT INTO t_identity SELECT number, number + 200, number + 200, number, 0, number, 'y', 'a' FROM numbers(30, 3);

SELECT 'final';
SELECT * FROM t_identity FINAL ORDER BY k SETTINGS max_block_size = 3;

SELECT 'group by';
SELECT k, min(mn), max(mx), sum(s), groupBitAnd(ba), anyLast(al), any(a), max(lc) FROM t_identity GROUP BY k ORDER BY k;

SELECT 'diff';
SELECT count() FROM
(
    SELECT * FROM t_identity FINAL
    EXCEPT
    SELECT k, min(mn), max(mx), sum(s), groupBitAnd(ba), anyLast(al), any(a), max(lc) FROM t_identity GROUP BY k
);

OPTIMIZE TABLE t_identity FINAL;
SELECT 'optimized';
SELECT * FROM t_identity ORDER BY k;

DROP TABLE t_identity;

DROP TABLE IF EXISTS t_normalizing;

CREATE TABLE t_normalizing
(
    k UInt32,
    uniq_arr SimpleAggregateFunction(groupUniqArrayArray, Array(UInt32)),
    sm SimpleAggregateFunction(sumMap, Tuple(Array(UInt32), Array(UInt64))),
    last_arr SimpleAggregateFunction(groupArrayLastArray(2), Array(UInt32)),
    limited_arr SimpleAggregateFunction(groupArrayArray(2), Array(UInt32)),
    long_str SimpleAggregateFunction(any, String),
    map_arr SimpleAggregateFunction(groupUniqArrayArrayMap, Map(UInt32, Array(UInt64)))
)
ENGINE = AggregatingMergeTree ORDER BY k;

INSERT INTO t_normalizing VALUES (1, [3, 1, 1, 2], ([2, 1, 2], [10, 20, 30]), [1, 2, 3, 4], [1, 2, 3, 4], repeat('a', 100), map(1, [1, 2, 2]));
INSERT INTO t_normalizing VALUES (2, [5, 5], ([1], [1]), [7], [7], repeat('b', 100), map(1, [1, 2, 3], 2, [4]));
INSERT INTO t_normalizing VALUES (2, [5, 6], ([1], [2]), [8, 9], [8, 9], repeat('b', 100), map(1, [3, 5], 2, [4, 5]));

SELECT 'normalizing final';
SELECT k, arraySort(uniq_arr), sm, last_arr, limited_arr, length(long_str), mapApply((x, y) -> (x, arraySort(y)), map_arr) FROM t_normalizing FINAL ORDER BY k SETTINGS max_threads = 1;

DROP TABLE t_normalizing;
