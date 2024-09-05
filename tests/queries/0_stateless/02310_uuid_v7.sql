SELECT '-- generateUUIDv7 --';
SELECT toTypeName(generateUUIDv7());
SELECT substring(hex(generateUUIDv7()), 13, 1); -- check version bits
SELECT bitAnd(bitShiftRight(toUInt128(generateUUIDv7()), 62), 3); -- check variant bits
SELECT generateUUIDv7(1) = generateUUIDv7(2);
SELECT generateUUIDv7() = generateUUIDv7(1);
SELECT generateUUIDv7(1) = generateUUIDv7(1);

SELECT '-- generateUUIDv7ThreadMonotonic --';
SELECT toTypeName(generateUUIDv7ThreadMonotonic());
SELECT substring(hex(generateUUIDv7ThreadMonotonic()), 13, 1); -- check version bits
SELECT bitAnd(bitShiftRight(toUInt128(generateUUIDv7ThreadMonotonic()), 62), 3); -- check variant bits
SELECT generateUUIDv7ThreadMonotonic(1) = generateUUIDv7ThreadMonotonic(2);
SELECT generateUUIDv7ThreadMonotonic() = generateUUIDv7ThreadMonotonic(1);
SELECT generateUUIDv7ThreadMonotonic(1) = generateUUIDv7ThreadMonotonic(1);

SELECT '-- generateUUIDv7NonMonotonic --';
SELECT toTypeName(generateUUIDv7NonMonotonic());
SELECT substring(hex(generateUUIDv7NonMonotonic()), 13, 1); -- check version bits
SELECT bitAnd(bitShiftRight(toUInt128(generateUUIDv7NonMonotonic()), 62), 3); -- check variant bits
SELECT generateUUIDv7NonMonotonic(1) = generateUUIDv7NonMonotonic(2);
SELECT generateUUIDv7NonMonotonic() = generateUUIDv7NonMonotonic(1);
SELECT generateUUIDv7NonMonotonic(1) = generateUUIDv7NonMonotonic(1);
