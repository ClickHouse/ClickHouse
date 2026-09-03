SELECT '-- bitShiftRight';
SELECT bitShiftRight(1, -1); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT bitShiftRight(toUInt32(1), 0);
SELECT bitShiftRight(toUInt32(1), 32);
SELECT bitShiftRight(toUInt32(1), 32 + 1);
SELECT bitShiftRight('hola', -1); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT bitShiftRight('hola', 0);
SELECT bitShiftRight('hola', 4 * 8);
SELECT bitShiftRight('hola', 4 * 8 + 1);
SELECT bitShiftRight(toFixedString('hola', 8), -1); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT bitShiftRight(toFixedString('hola', 8),  0);
SELECT bitShiftRight(toFixedString('hola', 8),  8 * 8);
SELECT bitShiftRight(toFixedString('hola', 8),  8 * 8 + 1);

SELECT '-- bitShiftLeft';
SELECT bitShiftLeft(1, -1); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT bitShiftLeft(toUInt32(1), 0);
SELECT bitShiftLeft(toUInt32(1), 32);
SELECT bitShiftLeft(toUInt32(1), 32 + 1);
SELECT bitShiftLeft('hola', -1); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT bitShiftLeft('hola', 0);
SELECT bitShiftLeft('hola', 4 * 8);
SELECT bitShiftLeft('hola', 4 * 8 + 1);
SELECT bitShiftLeft(toFixedString('hola', 8), -1); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT bitShiftLeft(toFixedString('hola', 8), 0);
SELECT bitShiftLeft(toFixedString('hola', 8), 8 * 8);
SELECT bitShiftLeft(toFixedString('hola', 8), 8 * 8 + 1);

SELECT 'OK';
