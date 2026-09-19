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

-- Error: JSON (Object) is comparable, but its values can mix runtime types in a single dynamic path,
-- so ranking them as plain Fields would disagree with the column ordering used by argMin/argMax and
-- ORDER BY. It is rejected for val at the top level and when nested, matching the set of types that
-- `canUseFieldForValueData` excludes from the Field-based path in the min/max family.
SET enable_json_type = 1;
SELECT argMaxMany(2)(number, ('{"a":' || toString(number) || '}')::JSON) FROM numbers(5); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT argMinMany(2)(number, ('{"a":' || toString(number) || '}')::JSON) FROM numbers(5); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT argMaxMany(2)(number, tuple(('{"a":' || toString(number) || '}')::JSON, number)) FROM numbers(5); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT argMinMany(2)(number, tuple(('{"a":' || toString(number) || '}')::JSON, number)) FROM numbers(5); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT argMaxMany(2)(number, [('{"a":' || toString(number) || '}')::JSON]) FROM numbers(5); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT argMinMany(2)(number, [('{"a":' || toString(number) || '}')::JSON]) FROM numbers(5); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

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

-- Variant arg is supported, also nested inside another type: arg values are kept in a column of
-- the arg type inside the state, so the active alternative is preserved, and the state is
-- serialized with the column-based binary serialization of Variant.
SELECT argMaxMany(2)(number::Variant(UInt64), number) FROM numbers(5);
SELECT argMinMany(2)(number::Variant(UInt64), number) FROM numbers(5);
SELECT argMaxMany(3)(v, number) FROM (SELECT number, CAST(if(number % 2 = 0, toString(number), 'x' || toString(number)), 'Variant(UInt64, String)') AS v FROM numbers(5));
SELECT argMinMany(3)(v, number) FROM (SELECT number, CAST(if(number % 2 = 0, toString(number), 'x' || toString(number)), 'Variant(UInt64, String)') AS v FROM numbers(5));
SELECT argMaxMany(2)(tuple(number::Variant(UInt64, String), number), number) FROM numbers(5);
SELECT argMinMany(2)([number::Variant(UInt64, String)], number) FROM numbers(5);
DROP TABLE IF EXISTS t_04266_argmaxmany_variant;
CREATE TABLE t_04266_argmaxmany_variant (s AggregateFunction(argMaxMany(3), Variant(UInt64, String), UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_04266_argmaxmany_variant SELECT argMaxManyState(3)(v, number) FROM (SELECT number, CAST(if(number % 2 = 0, toString(number), 'x' || toString(number)), 'Variant(UInt64, String)') AS v FROM numbers(5)) GROUP BY number % 2;
SELECT argMaxManyMerge(3)(s) FROM t_04266_argmaxmany_variant;
DROP TABLE t_04266_argmaxmany_variant;

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

-- JSON arg is supported, also nested inside another type, and the documents come back unchanged,
-- including a typed path holding NULL, which a Field-based state used to collapse into "path absent".
SET enable_json_type = 1;
SELECT argMaxMany(2)(('{"a":' || toString(number) || '}')::JSON, number) FROM numbers(5);
SELECT argMinMany(2)(('{"a":' || toString(number) || '}')::JSON, number) FROM numbers(5);
SELECT argMaxMany(2)(tuple(('{"a":' || toString(number) || '}')::JSON, number), number) FROM numbers(5);
SELECT argMinMany(2)([('{"a":' || toString(number) || '}')::JSON], number) FROM numbers(5);
SELECT argMaxMany(2)(j, number) FROM (SELECT number, if(number = 1, '{"b":2}', '{"a":1}')::JSON(a Nullable(Int64)) AS j FROM numbers(2));
SELECT argMinMany(2)(j, number) FROM (SELECT number, if(number = 1, '{"b":2}', '{"a":1}')::JSON(a Nullable(Int64)) AS j FROM numbers(2));
DROP TABLE IF EXISTS t_04266_argmaxmany_json;
CREATE TABLE t_04266_argmaxmany_json (s AggregateFunction(argMinMany(2), JSON, UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_04266_argmaxmany_json SELECT argMinManyState(2)(('{"a":' || toString(number) || '}')::JSON, number) FROM numbers(5) GROUP BY number % 2;
SELECT argMinManyMerge(2)(s) FROM t_04266_argmaxmany_json;
DROP TABLE t_04266_argmaxmany_json;

-- Float32 arg round-trips bit-exactly, like in argMax/argMin. A Field widens Float32 to Float64,
-- which quiets a signaling NaN payload; the state keeps arg in a column of the arg type instead.
SELECT reinterpretAsUInt32(argMax(x, val)) = reinterpretAsUInt32(arrayElement(argMaxMany(1)(x, val), 1)), hex(reinterpretAsUInt32(arrayElement(argMaxMany(1)(x, val), 1))) FROM (SELECT reinterpretAsFloat32(toUInt32(0x7F800001)) AS x, 1 AS val);
SELECT reinterpretAsUInt32(argMin(x, val)) = reinterpretAsUInt32(arrayElement(argMinMany(1)(x, val), 1)), hex(reinterpretAsUInt32(arrayElement(argMinMany(1)(x, val), 1))) FROM (SELECT reinterpretAsFloat32(toUInt32(0x7F800001)) AS x, 1 AS val);
SELECT hex(reinterpretAsUInt32(arrayElement(argMaxMany(1)(x, val), 1).1)) FROM (SELECT tuple(reinterpretAsFloat32(toUInt32(0x7F800001))) AS x, 1 AS val);
SELECT hex(reinterpretAsUInt32(arrayElement(argMaxManyMerge(1)(s), 1))) FROM (SELECT argMaxManyState(1)(x, val) AS s FROM (SELECT reinterpretAsFloat32(toUInt32(0x7F800001)) AS x, 1 AS val));

-- Many more accepted rows than N: every row replaces the heap root, so the arg column of the state
-- is compacted repeatedly, and the result must still be exact.
SELECT argMaxMany(2)(toString(number), number) FROM numbers(100000);
SELECT argMinMany(2)(toString(number), -toInt64(number)) FROM numbers(100000);
SELECT argMaxMany(1)(number, number) FROM numbers(100000);

-- NaN nested inside a composite val type follows the same rule as a top-level NaN: it is the worst
-- candidate, so it is evicted in favor of any real value and sorts last in the output. Field's own
-- ordering puts NaN after every real number, which used to make a nested NaN outrank real values.
SELECT argMaxMany(1)(arg, tuple(val, 0)) FROM (SELECT * FROM VALUES('arg String, val Float64', ('a',nan),('b',1),('c',3)));
SELECT argMinMany(1)(arg, tuple(val, 0)) FROM (SELECT * FROM VALUES('arg String, val Float64', ('a',nan),('b',1),('c',3)));
SELECT argMaxMany(3)(arg, tuple(val, 0)) FROM (SELECT * FROM VALUES('arg String, val Float64', ('a',nan),('b',1),('c',3)));
SELECT argMinMany(3)(arg, tuple(val, 0)) FROM (SELECT * FROM VALUES('arg String, val Float64', ('a',nan),('b',1),('c',3)));
SELECT argMaxMany(1)(arg, [val]) FROM (SELECT * FROM VALUES('arg String, val Float64', ('a',nan),('b',1),('c',3)));
SELECT argMinMany(1)(arg, [val]) FROM (SELECT * FROM VALUES('arg String, val Float64', ('a',nan),('b',1),('c',3)));
SELECT argMaxMany(1)(arg, tuple([val])) FROM (SELECT * FROM VALUES('arg String, val Float64', ('a',nan),('b',1),('c',3)));
SELECT argMinMany(1)(arg, tuple([val])) FROM (SELECT * FROM VALUES('arg String, val Float64', ('a',nan),('b',1),('c',3)));

-- The same on the merge path, where the nested NaN arrives from another partial state.
SELECT argMaxManyMerge(1)(s) FROM
(
    SELECT argMaxManyState(1)(arg, val) AS s FROM (SELECT 'a' AS arg, tuple(nan, 0) AS val)
    UNION ALL
    SELECT argMaxManyState(1)(arg, val) AS s FROM (SELECT 'b' AS arg, tuple(toFloat64(1), 0) AS val)
);
SELECT argMinManyMerge(1)(s) FROM
(
    SELECT argMinManyState(1)(arg, val) AS s FROM (SELECT 'a' AS arg, tuple(nan, 0) AS val)
    UNION ALL
    SELECT argMinManyState(1)(arg, val) AS s FROM (SELECT 'b' AS arg, tuple(toFloat64(1), 0) AS val)
);

-- A NULL stored inside a Dynamic arg is not a Nullable value: the Null combinator only wraps
-- Nullable arguments, so such rows are kept and the NULL is returned as-is (documented behaviour).
SELECT argMaxMany(2)(d, number) FROM (SELECT number, if(number = 4, NULL, number)::Dynamic AS d FROM numbers(5));
SELECT argMinMany(2)(d, number) FROM (SELECT number, if(number = 0, NULL, number)::Dynamic AS d FROM numbers(5));
