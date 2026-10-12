-- The Iceberg spec only defines hashing for `int` (32-bit), `long` (64-bit),
-- and `decimal(P,S)` with P <= 38 (see Appendix B of the Iceberg spec).
-- Passing wider types previously caused silent truncation and hash collisions
-- (https://github.com/ClickHouse/ClickHouse/issues/101875).

SELECT icebergHash(toInt128('18446744073709551617')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT icebergHash(toUInt128('18446744073709551617')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT icebergHash(toInt256('18446744073709551617')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT icebergHash(toUInt256('18446744073709551617')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT icebergHash(toDecimal256('14.20', 2)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT icebergBucket(5, toInt128('1')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT icebergBucket(5, toDecimal256('14.20', 2)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Spec-supported types still work after casting.
SELECT icebergHash(toInt64(34));
SELECT icebergHash(toDecimal128('14.20', 2));
SELECT icebergBucket(5, toInt64(34));
