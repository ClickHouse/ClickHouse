SELECT '-- generateUUIDv7 --';
SELECT toTypeName(generateUUIDv7());
SELECT substring(hex(generateUUIDv7()), 13, 1); -- extract version
SELECT bitAnd(bitShiftRight(toUInt128(generateUUIDv7()), 62), 3); -- extract variant
SELECT generateUUIDv7(1) = generateUUIDv7(2);
SELECT generateUUIDv7() = generateUUIDv7(1);
SELECT generateUUIDv7(1) = generateUUIDv7(1);

SELECT '-- generateUUIDv7WithFastCounter --';
SELECT toTypeName(generateUUIDv7WithFastCounter());
SELECT substring(hex(generateUUIDv7WithFastCounter()), 13, 1); -- extract version
SELECT bitAnd(bitShiftRight(toUInt128(generateUUIDv7WithFastCounter()), 62), 3); -- extract variant
SELECT generateUUIDv7WithFastCounter(1) = generateUUIDv7WithFastCounter(2);
SELECT generateUUIDv7WithFastCounter() = generateUUIDv7WithFastCounter(1);
SELECT generateUUIDv7WithFastCounter(1) = generateUUIDv7WithFastCounter(1);

SELECT '-- generateUUIDv7NonMonotonic --';
SELECT toTypeName(generateUUIDv7NonMonotonic());
SELECT substring(hex(generateUUIDv7NonMonotonic()), 13, 1); -- extract version
SELECT bitAnd(bitShiftRight(toUInt128(generateUUIDv7NonMonotonic()), 62), 3); -- extract variant
SELECT generateUUIDv7NonMonotonic(1) = generateUUIDv7NonMonotonic(2);
SELECT generateUUIDv7NonMonotonic() = generateUUIDv7NonMonotonic(1);
SELECT generateUUIDv7NonMonotonic(1) = generateUUIDv7NonMonotonic(1);
