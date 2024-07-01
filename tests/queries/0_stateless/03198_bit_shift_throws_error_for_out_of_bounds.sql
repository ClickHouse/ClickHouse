SELECT bitShiftRight(1, -1); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT bitShiftRight(toUInt8(1), number) FROM numbers(8 + 1) FORMAT Null;
SELECT bitShiftRight(toUInt8(1), 8 + 1); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT bitShiftRight('hola', -1); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT bitShiftRight('hola', number) FROM numbers(4 * 8 + 1) FORMAT Null;
SELECT bitShiftRight('hola', 4 * 8 + 1); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT bitShiftRight(toFixedString('hola', 8), -1); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT bitShiftRight(toFixedString('hola', 8), number) FROM numbers(8 * 8 + 1) FORMAT Null;
SELECT bitShiftRight(toFixedString('hola', 8),  8 * 8 + 1); -- { serverError ARGUMENT_OUT_OF_BOUND }

SELECT bitShiftLeft(1, -1); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT bitShiftLeft(toUInt8(1), number) FROM numbers(8 + 1) FORMAT Null;
SELECT bitShiftLeft(toUInt8(1), 8 + 1); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT bitShiftLeft('hola', -1); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT bitShiftLeft('hola', number) FROM numbers(4 * 8 + 1) FORMAT Null;
SELECT bitShiftLeft('hola', 4 * 8 + 1); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT bitShiftLeft(toFixedString('hola', 8), -1); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT bitShiftLeft(toFixedString('hola', 8), number) FROM numbers(8 * 8 + 1) FORMAT Null;
SELECT bitShiftLeft(toFixedString('hola', 8),  8 * 8 + 1); -- { serverError ARGUMENT_OUT_OF_BOUND }

SELECT 'OK';