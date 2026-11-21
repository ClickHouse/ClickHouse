-- A nested value inserted with `INSERT ... VALUES` through an expression, such as `[toUInt64(20000)]`, must be stored in the
-- `Variant` alternative whose type it has. It used to be stored in the first alternative (in the sorted order of the names)
-- that accepted it, so `[toUInt64(20000)]` went to `Array(Date)` and was read as `['2024-10-04']`, and a value that does not
-- fit in a `Date` wrapped around. `INSERT ... SELECT` of the same expression kept the type.

DROP TABLE IF EXISTS t_variant_values_scalar;
DROP TABLE IF EXISTS t_variant_values_array;
DROP TABLE IF EXISTS t_variant_values_tuple;
DROP TABLE IF EXISTS t_variant_values_map;

-- Scalars were already stored in the right alternative.
CREATE TABLE t_variant_values_scalar (id UInt8, v Variant(Date, UInt64)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_variant_values_scalar VALUES (1, toUInt64(20000)), (2, toDate(20000));
SELECT id, variantType(v), v FROM t_variant_values_scalar ORDER BY id;

CREATE TABLE t_variant_values_array (id UInt8, v Variant(Array(Date), Array(UInt64))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_variant_values_array VALUES (1, [toUInt64(20000)]), (2, [toUInt64(99999999999)]), (4, NULL);
INSERT INTO t_variant_values_array SELECT 3, CAST([toUInt64(20000)], 'Variant(Array(Date), Array(UInt64))');
SELECT id, variantType(v), v FROM t_variant_values_array ORDER BY id;

CREATE TABLE t_variant_values_tuple (id UInt8, v Variant(Tuple(Date), Tuple(UInt64))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_variant_values_tuple VALUES (1, tuple(toUInt64(20000)));
SELECT id, variantType(v), v FROM t_variant_values_tuple ORDER BY id;

CREATE TABLE t_variant_values_map (id UInt8, v Variant(Map(String, Date), Map(String, UInt64))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_variant_values_map VALUES (1, map('a', toUInt64(20000)));
SELECT id, variantType(v), v FROM t_variant_values_map ORDER BY id;

DROP TABLE t_variant_values_scalar;
DROP TABLE t_variant_values_array;
DROP TABLE t_variant_values_tuple;
DROP TABLE t_variant_values_map;
