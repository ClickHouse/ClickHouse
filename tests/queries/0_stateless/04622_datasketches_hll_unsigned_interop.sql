-- Tags: no-fasttest
-- no-fasttest: requires datasketches library

-- Apache DataSketches hashes uint8/16/32 through the same-width signed type
-- (sign-extending it to int64). The sketch built from an unsigned value above
-- the signed range of its type must therefore equal the sketch built from the
-- corresponding sign-extended Int64 value, and must differ from the sketch
-- built from the zero-extended UInt64 value.

SELECT 'UInt32 above signed range';
SELECT serializedHLL(toUInt32(3000000000)) = serializedHLL(toInt64(-1294967296));
SELECT serializedHLL(toUInt32(3000000000)) != serializedHLL(toUInt64(3000000000));

SELECT 'UInt16 above signed range';
SELECT serializedHLL(toUInt16(50000)) = serializedHLL(toInt64(-15536));
SELECT serializedHLL(toUInt16(50000)) != serializedHLL(toUInt64(50000));

SELECT 'UInt8 above signed range';
SELECT serializedHLL(toUInt8(200)) = serializedHLL(toInt64(-56));
SELECT serializedHLL(toUInt8(200)) != serializedHLL(toUInt64(200));

SELECT 'Values within the signed range are unaffected by the width';
SELECT serializedHLL(toUInt32(42)) = serializedHLL(toUInt64(42));
SELECT serializedHLL(toUInt16(42)) = serializedHLL(toInt64(42));

SELECT 'Signed types sign-extend to Int64';
SELECT serializedHLL(toInt32(-5)) = serializedHLL(toInt64(-5));
SELECT serializedHLL(toInt8(-5)) = serializedHLL(toInt64(-5));
