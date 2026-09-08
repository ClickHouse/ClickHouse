SET enable_analyzer = 1;

SELECT (number, number + 1) IN (materialize(number)) FROM numbers(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT (number, number + 1) NOT IN (materialize(number)) FROM numbers(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT (number, number + 1) IN (materialize(number)) FROM numbers(1) SETTINGS transform_null_in = 1; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT tuple(1, 2) IN (CAST(materialize(tuple(1, 2)), 'Dynamic')), tuple(1, 2) IN (CAST(materialize(tuple(2, 3)), 'Dynamic')), tuple(1, 2) IN (CAST(materialize(tuple(1, 2)), 'Variant(Tuple(UInt8, UInt8), UInt8)'));
SELECT (toNullable(1), 2) IN (materialize(NULL)), (toNullable(1), 2) NOT IN (materialize(NULL)), toTypeName((toNullable(1), 2) IN (materialize(NULL)));
