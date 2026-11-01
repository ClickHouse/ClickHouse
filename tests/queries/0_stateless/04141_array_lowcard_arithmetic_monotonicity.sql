-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/103691
--
-- Logical error 'Arguments of `plus` have incorrect data types' was thrown when
-- a `MergeTree` table had `Array(LowCardinality(...))` in its sort key and a
-- WHERE clause invoked `plus` / `minus` between that column and an `Array`
-- constant of a different element type. `KeyCondition::getMonotonicityForRange`
-- bypassed the framework's `LowCardinality` default implementation and called
-- `Base::executeImpl` directly with `Array(LowCardinality(Float64))`, which the
-- inner numeric dispatch in `FunctionBinaryArithmetic::executeImpl2` cannot
-- handle (the dispatch is over scalar numeric types only).

SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_array_lc_monotonicity;

CREATE TABLE t_array_lc_monotonicity (a Array(LowCardinality(Float64)))
    ENGINE = MergeTree() ORDER BY a;

INSERT INTO t_array_lc_monotonicity VALUES ([1.0]), ([3.0]);

-- const + variable: triggers the const + variable branch of getMonotonicityForRange.
SELECT * FROM t_array_lc_monotonicity
    WHERE plus(CAST([0] AS Array(Int16)), a) < CAST([5.0] AS Array(Float64))
    ORDER BY a;

-- variable + const: triggers the variable + const branch.
SELECT * FROM t_array_lc_monotonicity
    WHERE plus(a, CAST([0] AS Array(Int16))) < CAST([5.0] AS Array(Float64))
    ORDER BY a;

-- const - variable: same path for minus.
SELECT * FROM t_array_lc_monotonicity
    WHERE minus(CAST([0] AS Array(Int16)), a) > CAST([-5.0] AS Array(Float64))
    ORDER BY a;

-- variable - const: same path for minus.
SELECT * FROM t_array_lc_monotonicity
    WHERE minus(a, CAST([0] AS Array(Int16))) < CAST([5.0] AS Array(Float64))
    ORDER BY a;

DROP TABLE t_array_lc_monotonicity;
