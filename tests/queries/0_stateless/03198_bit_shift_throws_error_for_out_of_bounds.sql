SELECT '-- bitShiftRight';
SELECT bitShiftRight(1, -1); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT bitShiftRight(toUInt8(1), 8 + 1); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT bitShiftRight('hola', -1); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT bitShiftRight('hola', 4 * 8 + 1); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT bitShiftRight(toFixedString('hola', 8), -1); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT bitShiftRight(toFixedString('hola', 8),  8 * 8 + 1); -- { serverError ARGUMENT_OUT_OF_BOUND }

SELECT '-- bitShiftLeft';
SELECT bitShiftLeft(1, -1); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT bitShiftLeft(toUInt8(1), 8 + 1); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT bitShiftLeft('hola', -1); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT bitShiftLeft('hola', 4 * 8 + 1); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT bitShiftLeft(toFixedString('hola', 8), -1); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT bitShiftLeft(toFixedString('hola', 8),  8 * 8 + 1); -- { serverError ARGUMENT_OUT_OF_BOUND }

SELECT 'OK';