-- The serialized form of a regr_* state must be a function of the state alone: the same rows must
-- give the same bytes, and a state must survive a round trip through a column.

SELECT 'the serialized state is exactly its values';
SELECT length(toString(regr_slopeState(y, x))) FROM VALUES('x UInt8, y Int32', (7, 9));
SELECT hex(regr_slopeState(y, x)) FROM VALUES('x UInt8, y Int32', (7, 9));
SELECT length(toString(regr_slopeState(y, x))) FROM VALUES('x Float64, y Float64', (7, 9));

SELECT 'the same rows give one state';
SELECT uniqExact(s) FROM (SELECT number % 500 AS g, regr_slopeState(y, x) AS s
    FROM (SELECT number, toUInt8(7) AS x, toUInt8(9) AS y FROM numbers(50000)) GROUP BY g);
-- control: Float64 arguments
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
