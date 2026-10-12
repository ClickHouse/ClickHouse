-- `__bitSwapLastTwo` accepts only `UInt8` (also `Bool`, and `Nullable` or `LowCardinality` of it). Other argument types are
-- rejected when the query is analyzed, also when no row reaches the function.

DROP TABLE IF EXISTS t_bit_swap_last_two;
CREATE TABLE t_bit_swap_last_two (c0 UInt8) ENGINE = Memory;

EXPLAIN header = 1 SELECT __bitSwapLastTwo(toFloat64(c0)) FROM t_bit_swap_last_two; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT __bitSwapLastTwo(toFloat64(c0)) FROM t_bit_swap_last_two; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT __bitSwapLastTwo(toFloat32(c0)) FROM t_bit_swap_last_two; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT __bitSwapLastTwo(toInt8(c0)) FROM t_bit_swap_last_two; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT __bitSwapLastTwo(toUInt16(c0)) FROM t_bit_swap_last_two; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT __bitSwapLastTwo(toInt128(c0)) FROM t_bit_swap_last_two; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT __bitSwapLastTwo(toNullable(toUInt64(c0))) FROM t_bit_swap_last_two; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT __bitSwapLastTwo(toLowCardinality(toUInt16(c0))) FROM t_bit_swap_last_two; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- The same error when rows reach the function.
SELECT __bitSwapLastTwo(toFloat64(number)) FROM numbers(2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT number, __bitSwapLastTwo(toUInt8(number)), __bitSwapLastTwo(toBool(number)), __bitSwapLastTwo(toNullable(toUInt8(number))),
    __bitSwapLastTwo(toLowCardinality(toUInt8(number))), __bitSwapLastTwo(NULL)
FROM numbers(4) ORDER BY number;

-- A `Dynamic` argument is checked against the type of the values it holds.
SELECT __bitSwapLastTwo(materialize(toFloat64(1)::Dynamic)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT __bitSwapLastTwo(materialize(toFloat64(1)::Dynamic)) SETTINGS dynamic_throw_on_type_mismatch = 0;
SELECT __bitSwapLastTwo(materialize(toUInt8(1)::Dynamic));

DROP TABLE t_bit_swap_last_two;
