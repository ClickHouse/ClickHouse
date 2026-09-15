-- The serialized form of an aggregate state must be a function of its values alone. The regr_*
-- state keeps each argument in its own column type, so for narrow types its layout has a hole
-- before the Float64 sums; writing the state as a raw struct put those never-initialised bytes into
-- the format, and DISTINCT / GROUP BY / uniqExact over an AggregateFunction(regr_*) column then
-- split states holding the same fit.

SELECT 'the serialized state is exactly its values';
-- 8 (count) + 1 (x0 UInt8) + 4 (y0 Int32) + 5 * 8 (the sums) = 53; sizeof(state) is 56
SELECT length(toString(regr_slopeState(y, x))) FROM VALUES('x UInt8, y Int32', (7, 9));
SELECT hex(regr_slopeState(y, x)) FROM VALUES('x UInt8, y Int32', (7, 9));
-- a hole-free layout is unchanged: 8 (count) + 8 (x0) + 8 (y0) + 5 * 8 (the sums) = 64 = sizeof(state)
SELECT length(toString(regr_slopeState(y, x))) FROM VALUES('x Float64, y Float64', (7, 9));

SELECT 'the same rows give one state';
SELECT uniqExact(s) FROM (SELECT number % 500 AS g, regr_slopeState(y, x) AS s
    FROM (SELECT number, toUInt8(7) AS x, toUInt8(9) AS y FROM numbers(50000)) GROUP BY g);
-- control: a layout with no hole was always correct and must stay so
SELECT uniqExact(s) FROM (SELECT number % 500 AS g, regr_slopeState(y, x) AS s
    FROM (SELECT number, toFloat64(7) AS x, toFloat64(9) AS y FROM numbers(50000)) GROUP BY g);

SELECT 'a state written to a column and read back merges into the same fit';
DROP TABLE IF EXISTS t_regr_state;
CREATE TABLE t_regr_state
(
    slope AggregateFunction(regr_slope, Int32, UInt8),
    syy AggregateFunction(regr_syy, Int32, UInt8)
) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_regr_state SELECT regr_slopeState(y, x), regr_syyState(y, x) FROM VALUES('x UInt8, y Int32', (1, 5), (2, 7), (3, 9));
INSERT INTO t_regr_state SELECT regr_slopeState(y, x), regr_syyState(y, x) FROM VALUES('x UInt8, y Int32', (4, 11), (5, 13));
-- y = 2x + 3 over y = 5, 7, 9, 11, 13: the slope is 2, and the squared deviations of y sum to 40
SELECT regr_slopeMerge(slope), regr_syyMerge(syy) FROM t_regr_state;
DROP TABLE t_regr_state;
