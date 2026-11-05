-- 128-bit / 256-bit `num_buckets` arguments must be rejected up-front.
-- Previously they were read via IColumn::getUInt / getInt and silently
-- truncated to 64 bits, so e.g. toUInt128('18446744073709551621') (which
-- equals 2^64 + 5) became a 5-bucket hash. Fix: isNativeInteger() type
-- check in `getReturnTypeImpl`.

SELECT jumpConsistentHash(256, toUInt128('18446744073709551621')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT jumpConsistentHash(256, toInt128('18446744073709551621'));  -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT jumpConsistentHash(256, toUInt256('18446744073709551621')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT jumpConsistentHash(256, toInt256('18446744073709551621'));  -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT kostikConsistentHash(256, toUInt128('18446744073709551621')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT yandexConsistentHash(256, toInt128('18446744073709551621'));  -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Positive: native-integer `num_buckets` still works.
SELECT jumpConsistentHash(toUInt64(18446744073709551615), 100);
SELECT jumpConsistentHash(256, toUInt8(10));
SELECT jumpConsistentHash(256, toInt32(10));
SELECT kostikConsistentHash(256, toUInt32(10));
SELECT yandexConsistentHash(256, toUInt32(10));

-- Boundary: smallest legal `num_buckets`.
SELECT jumpConsistentHash(0, 1);
