-- Tags: no-fasttest
-- no-fasttest: requires datasketches library

-- 128/256-bit integers have no Apache DataSketches primitive overload, so hashing them
-- would produce ClickHouse-only sketches that other DataSketches producers cannot
-- reproduce. They must be rejected instead of silently falling back to raw-byte hashing.

SELECT serializedHLL(toInt128(1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT serializedHLL(toUInt128(1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT serializedHLL(toInt256(1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT serializedHLL(toUInt256(1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT serializedHLL(12)(toUInt256(number)) FROM numbers(10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- An explicit conversion to a supported type works.
SELECT cardinalityFromHLL(serializedHLL(toString(toUInt256(number)))) BETWEEN 9 AND 11 FROM numbers(10);
