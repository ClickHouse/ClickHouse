-- Basic argMaxMany: returns top N args sorted by val descending
SELECT argMaxMany(2)(arg, val) FROM (SELECT * FROM VALUES('arg String, val UInt64', ('a',1),('b',3),('c',2)));

-- Basic argMinMany: returns bottom N args sorted by val ascending
SELECT argMinMany(2)(arg, val) FROM (SELECT * FROM VALUES('arg String, val UInt64', ('a',1),('b',3),('c',2)));

-- N larger than number of rows: return all rows sorted
SELECT argMaxMany(10)(number, number) FROM numbers(3);
SELECT argMinMany(10)(number, number) FROM numbers(3);

-- Single element
SELECT argMaxMany(1)(arg, val) FROM (SELECT * FROM VALUES('arg String, val UInt64', ('a',1),('b',3),('c',2)));
SELECT argMinMany(1)(arg, val) FROM (SELECT * FROM VALUES('arg String, val UInt64', ('a',1),('b',3),('c',2)));

-- NULL val values are excluded (consistent with argMax/argMin)
SELECT argMaxMany(3)(a, b) FROM (SELECT * FROM VALUES('a String, b Nullable(Int64)', ('x',1),('y',NULL),('z',3),('w',2)));
SELECT argMinMany(3)(a, b) FROM (SELECT * FROM VALUES('a String, b Nullable(Int64)', ('x',1),('y',NULL),('z',3),('w',2)));

-- NULL arg values are skipped (consistent with argMax/argMin null-aware wrapping)
SELECT argMaxMany(2)(a, b) FROM (SELECT * FROM VALUES('a Nullable(String), b Int64', ('x',1),(NULL,3),('z',2)));
SELECT argMinMany(2)(a, b) FROM (SELECT * FROM VALUES('a Nullable(String), b Int64', ('x',1),(NULL,3),('z',2)));

-- Empty input
SELECT argMaxMany(5)(number, number) FROM numbers(0);
SELECT argMinMany(5)(number, number) FROM numbers(0);

-- Numeric types for arg and float for val
SELECT argMaxMany(3)(toInt32(number), toFloat64(number)) FROM numbers(5);
SELECT argMinMany(3)(toInt32(number), toFloat64(number)) FROM numbers(5);

-- Tie-breaking: result length must be N even when all vals are equal
SELECT length(argMaxMany(2)(arg, val)) FROM (SELECT * FROM VALUES('arg String, val UInt64', ('a',1),('b',1),('c',1)));
SELECT length(argMinMany(2)(arg, val)) FROM (SELECT * FROM VALUES('arg String, val UInt64', ('a',1),('b',1),('c',1)));

-- Error: N must be positive
SELECT argMaxMany(0)(number, number) FROM numbers(5); -- { serverError BAD_ARGUMENTS }
SELECT argMinMany(-1)(number, number) FROM numbers(5); -- { serverError BAD_ARGUMENTS }

-- Error: Dynamic and Variant types are rejected for the val argument
SET allow_experimental_dynamic_type = 1;
SELECT argMaxMany(2)(number, number::Dynamic) FROM numbers(5); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT argMinMany(2)(number, number::Dynamic) FROM numbers(5); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SET allow_experimental_variant_type = 1;
SELECT argMaxMany(2)(number, number::Variant(UInt64)) FROM numbers(5); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT argMinMany(2)(number, number::Variant(UInt64)) FROM numbers(5); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Error: Dynamic/Variant nested anywhere inside the val type are also rejected, matching argMin/argMax.
-- Tuple, Array, Map, Nullable, and LowCardinality forward isComparable to their children, so a
-- top-level-only guard would let these through even though the underlying values can mix runtime types.
SELECT argMaxMany(2)(number, tuple(number::Dynamic, number)) FROM numbers(5); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT argMinMany(2)(number, tuple(number::Dynamic, number)) FROM numbers(5); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT argMaxMany(2)(number, [number::Dynamic]) FROM numbers(5); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT argMinMany(2)(number, [number::Dynamic]) FROM numbers(5); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT argMaxMany(2)(number, tuple(number::Variant(UInt64, String), number)) FROM numbers(5); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT argMinMany(2)(number, tuple(number::Variant(UInt64, String), number)) FROM numbers(5); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT argMaxMany(2)(number, [number::Variant(UInt64, String)]) FROM numbers(5); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT argMinMany(2)(number, [number::Variant(UInt64, String)]) FROM numbers(5); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- NaN val ranks as the worst candidate (consistent with argMax/argMin), so it is evicted in
-- favor of real values and never lingers in the heap.
SELECT argMaxMany(1)(arg, val) FROM (SELECT * FROM VALUES('arg String, val Float64', ('a',nan),('b',1),('c',3)));
SELECT argMinMany(1)(arg, val) FROM (SELECT * FROM VALUES('arg String, val Float64', ('a',nan),('b',1),('c',3)));
-- NaN sorts last in the output when there are fewer than N real values.
SELECT argMaxMany(3)(arg, val) FROM (SELECT * FROM VALUES('arg String, val Float64', ('a',nan),('b',1),('c',3)));
SELECT argMinMany(3)(arg, val) FROM (SELECT * FROM VALUES('arg String, val Float64', ('a',nan),('b',1),('c',3)));

-- The N parameter is part of the state type: states built with a different N are not interchangeable.
SELECT toTypeName(argMaxManyState(2)(number, number)) FROM numbers(3);
SELECT toTypeName(argMaxManyState(3)(number, number)) FROM numbers(3);

-- Window aggregation reuses the same state across a growing frame: insertResultInto must not
-- corrupt the heap. This must match the equivalent ORDER BY ... LIMIT computed per prefix.
SELECT argMaxMany(2)(number, number) OVER (ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM numbers(5);
SELECT argMinMany(2)(number, number) OVER (ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM numbers(5);

-- Merge path with NaN: a partial state whose only val is NaN must still be beaten by a real val
-- from another partial state, regardless of merge order. NaN ranks as the worst candidate for both
-- argMaxMany (largest wins) and argMinMany (smallest wins). Reproduces a bug where the merge path
-- (addEntry) used raw Field ordering, which treats NaN as the largest value, and so kept the NaN.
SELECT argMaxManyMerge(1)(s) FROM
(
    SELECT argMaxManyState(1)(arg, val) AS s FROM (SELECT 'a' AS arg, nan AS val)
    UNION ALL
    SELECT argMaxManyState(1)(arg, val) AS s FROM (SELECT 'b' AS arg, toFloat64(1) AS val)
);
SELECT argMinManyMerge(1)(s) FROM
(
    SELECT argMinManyState(1)(arg, val) AS s FROM (SELECT 'a' AS arg, nan AS val)
    UNION ALL
    SELECT argMinManyState(1)(arg, val) AS s FROM (SELECT 'b' AS arg, toFloat64(1) AS val)
);

-- Error: Variant anywhere inside the arg type is rejected: arg values are stored in the state
-- as plain Fields, and SerializationVariant does not implement Field-based binary serialization,
-- so serializing the state (argMaxManyState, distributed merges) would throw. A Field also cannot
-- record which variant alternative was active, so the result could reconstruct a different one.
SELECT argMaxMany(2)(number::Variant(UInt64), number) FROM numbers(5); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT argMinMany(2)(number::Variant(UInt64), number) FROM numbers(5); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT argMaxMany(2)(tuple(number::Variant(UInt64, String), number), number) FROM numbers(5); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT argMinMany(2)(tuple(number::Variant(UInt64, String), number), number) FROM numbers(5); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT argMaxMany(2)([number::Variant(UInt64, String)], number) FROM numbers(5); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT argMinMany(2)([number::Variant(UInt64, String)], number) FROM numbers(5); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Dynamic arg is supported, including through state serialization: SerializationDynamic encodes
-- the value type together with the value. Round-trip the state through a MergeTree table to
-- force binary serialization and deserialization of the state.
SELECT argMaxMany(2)(number::Dynamic, number) FROM numbers(5);
SELECT argMinMany(2)(number::Dynamic, number) FROM numbers(5);
DROP TABLE IF EXISTS t_04266_argmaxmany_dynamic;
CREATE TABLE t_04266_argmaxmany_dynamic (s AggregateFunction(argMaxMany(2), Dynamic, UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_04266_argmaxmany_dynamic SELECT argMaxManyState(2)(number::Dynamic, number) FROM numbers(5) GROUP BY number % 2;
SELECT argMaxManyMerge(2)(s) FROM t_04266_argmaxmany_dynamic;
DROP TABLE t_04266_argmaxmany_dynamic;
