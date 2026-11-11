SET allow_experimental_dynamic_type = 1;
SET allow_experimental_variant_type = 1;

-- min/max with nested Dynamic in Tuple
SELECT min(tuple(number::Dynamic, number)) FROM numbers(3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT max(tuple(number::Dynamic, number)) FROM numbers(3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- min/max with nested Dynamic in Array
SELECT min([number::Dynamic]) FROM numbers(3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT max([number::Dynamic]) FROM numbers(3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- min/max with nested Dynamic in Map
SELECT min(map('a', number::Dynamic)) FROM numbers(3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- min/max with nested Variant in Tuple
SELECT min(tuple(number::Variant(UInt64, String), number)) FROM numbers(3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT max(tuple(number::Variant(UInt64, String), number)) FROM numbers(3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- min/max with nested Variant in Array
SELECT min([number::Variant(UInt64, String)]) FROM numbers(3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- argMin/argMax with nested Dynamic in value argument
SELECT argMin(1, tuple(number::Dynamic, number)) FROM numbers(3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT argMax(1, tuple(number::Dynamic, number)) FROM numbers(3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT argMin(1, [number::Dynamic]) FROM numbers(3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- argMin/argMax with nested Variant in value argument
SELECT argMin(1, tuple(number::Variant(UInt64, String), number)) FROM numbers(3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Combinator ArgMin/ArgMax with nested Dynamic
SELECT sumArgMin(number, tuple(number::Dynamic, number)) FROM numbers(3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT sumArgMax(number, tuple(number::Dynamic, number)) FROM numbers(3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT sumArgMin(number, [number::Dynamic]) FROM numbers(3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Combinator ArgMin/ArgMax with nested Variant
SELECT sumArgMin(number, tuple(number::Variant(UInt64, String), number)) FROM numbers(3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- minmax index with nested Dynamic in Tuple
CREATE TABLE test_minmax_nested_dynamic (x Tuple(Dynamic, UInt64), INDEX idx x TYPE minmax) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- minmax index with nested Dynamic in Array
CREATE TABLE test_minmax_nested_dynamic_arr (x Array(Dynamic), INDEX idx x TYPE minmax) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- minmax index with nested Variant in Tuple
CREATE TABLE test_minmax_nested_variant (x Tuple(Variant(UInt64, String), UInt64), INDEX idx x TYPE minmax) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- Top-level Dynamic/Variant should still be rejected (existing behavior)
SELECT min(number::Dynamic) FROM numbers(3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT max(number::Dynamic) FROM numbers(3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT argMin(1, number::Dynamic) FROM numbers(3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT sumArgMin(number, number::Dynamic) FROM numbers(3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
CREATE TABLE test_minmax_top_dynamic (x Dynamic, INDEX idx x TYPE minmax) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
