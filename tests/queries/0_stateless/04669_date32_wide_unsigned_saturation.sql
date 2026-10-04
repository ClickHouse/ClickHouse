-- Timestamps are interpreted in the session time zone, so pin it to make the results reproducible.
SET session_timezone = 'UTC';

-- Timestamps above the Date32 range saturate at `9999-12-31` for every accepted numeric type,
-- including the wide unsigned integers, where narrowing to Int64 before the comparison would wrap around.
SELECT toDate32(toUInt8(255));
SELECT toDate32(toUInt16(65535));
SELECT toDate32(toUInt32(4294967295));
SELECT toDate32(toUInt64(18446744073709551615));
SELECT toDate32(toUInt128('340282366920938463463374607431768211455'));
SELECT toDate32(toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639935'));
SELECT toDate32(toUInt128('18446744073709551616'));
SELECT toDate32(toInt64(9223372036854775807));
SELECT toDate32(toInt128('170141183460469231731687303715884105727'));
SELECT toDate32(toInt256('57896044618658097711785492504343953926634992332820282019728792003956564819967'));

-- The boundary itself and the values just below it are unaffected.
SELECT toDate32(toUInt128(2932896)), toDate32(toUInt128(2932897)), toDate32(toUInt128(253402300799)), toDate32(toUInt128(253402300800));

-- Values below the range saturate at `0000-01-01`.
SELECT toDate32(toInt64(-9223372036854775808));
SELECT toDate32(toInt256('-57896044618658097711785492504343953926634992332820282019728792003956564819968'));
