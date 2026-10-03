-- Tags: no-fasttest
-- no-fasttest: requires datasketches library

-- Decimal types pass the isNumber check but have no instantiation in the numeric type dispatch.
-- They must be rejected with ILLEGAL_TYPE_OF_ARGUMENT instead of a logical error (fuzzer-found).
SELECT serializedQuantiles(toDecimal32(1.5, 2)) FROM numbers(10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT serializedQuantiles(toDecimal256(10.0001, 9) / number) FROM numbers(1, 10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT serializedTDigest(toDecimal64(1.5, 2)) FROM numbers(10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT serializedTDigest(toDecimal128(1.5, 2)) FROM numbers(10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT serializedHLL(toDecimal32(1.5, 2)) FROM numbers(10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT serializedHLL(toDecimal256(1.5, 2)) FROM numbers(10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- The same through combinators, as found by the fuzzer.
SELECT serializedQuantilesDistinctOrDefaultDistinctOrNull(toDecimal256(10.0001, 9) / number) FROM numbers(1, 10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
