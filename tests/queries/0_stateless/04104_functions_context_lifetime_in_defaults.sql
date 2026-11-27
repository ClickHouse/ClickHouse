-- Test that functions using context work correctly in deferred execution paths
-- (DEFAULT/MATERIALIZED expressions, MergeTree engines).
-- These functions internally call FunctionFactory::get which requires live context.
-- Using WithContext (weak_ptr) instead of ContextPtr would cause
-- "Context has expired" exceptions in these paths.

-- Tuple functions: dotProduct in DEFAULT expression
DROP TABLE IF EXISTS t_tuple_func_default;
CREATE TABLE t_tuple_func_default
(
    a Tuple(Int32, Int32),
    b Tuple(Int32, Int32),
    dot_product DEFAULT dotProduct(a, b)
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_tuple_func_default (a, b) VALUES ((1, 2), (3, 4));
INSERT INTO t_tuple_func_default (a, b) VALUES ((5, 6), (7, 8));

SELECT a, b, dot_product FROM t_tuple_func_default ORDER BY dot_product;

DROP TABLE t_tuple_func_default;

-- Tuple functions: L2Distance in MATERIALIZED column
DROP TABLE IF EXISTS t_l2_distance_materialized;
CREATE TABLE t_l2_distance_materialized
(
    a Tuple(Float64, Float64),
    b Tuple(Float64, Float64),
    dist MATERIALIZED L2Distance(a, b)
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_l2_distance_materialized (a, b) VALUES ((0, 0), (3, 4));
INSERT INTO t_l2_distance_materialized (a, b) VALUES ((1, 1), (1, 1));

SELECT a, b, dist FROM t_l2_distance_materialized ORDER BY dist;

DROP TABLE t_l2_distance_materialized;

-- Tuple functions: tupleHammingDistance in DEFAULT expression
DROP TABLE IF EXISTS t_hamming_default;
CREATE TABLE t_hamming_default
(
    a Tuple(UInt8, UInt8, UInt8),
    b Tuple(UInt8, UInt8, UInt8),
    hamming DEFAULT tupleHammingDistance(a, b)
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_hamming_default (a, b) VALUES ((1, 2, 3), (1, 2, 3));
INSERT INTO t_hamming_default (a, b) VALUES ((1, 2, 3), (4, 5, 6));

SELECT a, b, hamming FROM t_hamming_default ORDER BY hamming;

DROP TABLE t_hamming_default;

-- Arithmetic with DateTime in DEFAULT (exercises FunctionBinaryArithmetic)
DROP TABLE IF EXISTS t_datetime_arithmetic_default;
CREATE TABLE t_datetime_arithmetic_default
(
    ts DateTime DEFAULT now(),
    ts_plus_hour DEFAULT ts + toIntervalHour(1)
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_datetime_arithmetic_default (ts) VALUES ('2024-01-01 00:00:00');

SELECT ts, ts_plus_hour FROM t_datetime_arithmetic_default;

DROP TABLE t_datetime_arithmetic_default;

-- formatRow in DEFAULT expression
DROP TABLE IF EXISTS t_format_row_default;
CREATE TABLE t_format_row_default
(
    x UInt32,
    y String,
    formatted DEFAULT formatRowNoNewline('CSV', x, y)
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_format_row_default (x, y) VALUES (42, 'hello');
INSERT INTO t_format_row_default (x, y) VALUES (100, 'world');

SELECT x, y, formatted FROM t_format_row_default ORDER BY x;

DROP TABLE t_format_row_default;

-- Tuple functions in MergeTree ORDER BY expression
DROP TABLE IF EXISTS t_tuple_order_by;
CREATE TABLE t_tuple_order_by
(
    a Tuple(Float64, Float64),
    b Tuple(Float64, Float64)
)
ENGINE = MergeTree ORDER BY L1Distance(a, b);

INSERT INTO t_tuple_order_by VALUES ((0, 0), (1, 1));
INSERT INTO t_tuple_order_by VALUES ((0, 0), (3, 4));

SELECT a, b, L1Distance(a, b) as dist FROM t_tuple_order_by ORDER BY dist;

DROP TABLE t_tuple_order_by;

-- least/greatest in DEFAULT expression (exercises LeastGreatestOverloadResolver,
-- which builds FunctionBinaryArithmetic for two numeric arguments and needs
-- live context at execution time).
DROP TABLE IF EXISTS t_least_greatest_default;
CREATE TABLE t_least_greatest_default
(
    x Int32,
    y Int32,
    min_val DEFAULT least(x, y),
    max_val DEFAULT greatest(x, y)
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_least_greatest_default (x, y) VALUES (3, 5);
INSERT INTO t_least_greatest_default (x, y) VALUES (8, 2);

SELECT x, y, min_val, max_val FROM t_least_greatest_default ORDER BY x;

DROP TABLE t_least_greatest_default;

-- least/greatest with three or more arguments uses the generic implementation,
-- which also receives a context at construction time.
DROP TABLE IF EXISTS t_least_greatest_generic_default;
CREATE TABLE t_least_greatest_generic_default
(
    x Int32,
    y Int32,
    z Int32,
    min_val DEFAULT least(x, y, z),
    max_val DEFAULT greatest(x, y, z)
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_least_greatest_generic_default (x, y, z) VALUES (3, 5, 1);
INSERT INTO t_least_greatest_generic_default (x, y, z) VALUES (8, 2, 6);

SELECT x, y, z, min_val, max_val FROM t_least_greatest_generic_default ORDER BY x;

DROP TABLE t_least_greatest_generic_default;

-- CAST in DEFAULT expression (exercises CastOverloadResolverImpl, which uses
-- WithContext and would throw "Context has expired" if the resolver is
-- evaluated in a deferred path with an expired planning context).
DROP TABLE IF EXISTS t_cast_default;
CREATE TABLE t_cast_default
(
    s String,
    n DEFAULT CAST(s AS UInt64),
    a DEFAULT accurateCast(s, 'UInt32')
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_cast_default (s) VALUES ('123');
INSERT INTO t_cast_default (s) VALUES ('456');

SELECT s, n, a FROM t_cast_default ORDER BY s;

DROP TABLE t_cast_default;

-- CAST in MATERIALIZED column.
DROP TABLE IF EXISTS t_cast_materialized;
CREATE TABLE t_cast_materialized
(
    s String,
    n MATERIALIZED CAST(s AS UInt64)
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_cast_materialized (s) VALUES ('100');
INSERT INTO t_cast_materialized (s) VALUES ('200');

SELECT s, n FROM t_cast_materialized ORDER BY s;

DROP TABLE t_cast_materialized;
