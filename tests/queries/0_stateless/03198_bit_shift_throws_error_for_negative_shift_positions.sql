SELECT bitShiftRight(1, -1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT bitShiftRight('hola', -1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT bitShiftRight(toFixedString('hola', 10), -1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT bitShiftLeft(1, -1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT bitShiftLeft('hola', -1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT bitShiftLeft(toFixedString('hola', 10), -1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }