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
