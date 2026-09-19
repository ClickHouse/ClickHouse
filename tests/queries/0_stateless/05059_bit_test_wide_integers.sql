SELECT
    bitTest(bitShiftLeft(toUInt128(1), 64), 64) = 1,
    bitTest(bitShiftLeft(toUInt128(1), 127), toUInt64(127)) = 1,
    bitTest(bitShiftLeft(toUInt256(1), 128), 128) = 1,
    bitTest(bitShiftLeft(toUInt256(1), 255), toUInt64(255)) = 1,
    bitTest(toInt128(-1), 127) = 1,
    bitTest(toInt256(-1), 255) = 1,
    bitTest(toUInt256(1), toInt8(0)) = 1,
    bitTest(bitShiftLeft(toUInt256(1), 127), toInt8(127)) = 1;

SELECT
    bitTestAll(bitShiftLeft(toUInt128(3), 100), toUInt64(100), toUInt64(101)) = 1,
    bitTestAll(bitShiftLeft(toUInt256(3), 200), 200, 201) = 1,
    bitTestAll(toInt128(-1), 0, 127) = 1,
    bitTestAll(toInt256(-1), 0, 255) = 1,
    bitTestAny(bitShiftLeft(toUInt128(1), 127), 0, 127) = 1,
    bitTestAny(bitShiftLeft(toUInt256(1), 255), 0, toUInt64(255)) = 1,
    bitTestAny(toInt128(-1), 127) = 1,
    bitTestAny(toInt256(-1), 255) = 1;

SELECT bitTestAll(toInt64(-1), 63);

SELECT
    bitTest(toUInt128(0), 127),
    bitTest(bitShiftLeft(toUInt256(1), 200), 199),
    bitTestAll(bitShiftLeft(toUInt128(3), 100), 100, 102),
    bitTestAny(bitShiftLeft(toUInt256(1), 200), 199),
    bitTestAll(bitShiftLeft(toInt256(1), 255), 254, 255),
    bitTestAny(bitShiftLeft(toInt128(1), 127), 126);

SELECT
    countIf(bitTest(bitShiftLeft(toUInt128(1), number), number) = 1) = 128,
    countIf(bitTest(bitShiftLeft(toInt128(1), number), number) = 1) = 128
FROM numbers(128);

SELECT
    countIf(bitTestAll(bitShiftLeft(toUInt256(1), number), number) = 1) = 256,
    countIf(bitTestAll(bitShiftLeft(toInt256(1), number), number) = 1) = 256,
    countIf(bitTestAny(bitShiftLeft(toUInt256(1), number), number, 0) = 1) = 256,
    countIf(bitTestAny(bitShiftLeft(toInt256(1), number), number) = 1) = 256
FROM numbers(256);

SELECT bitTest(bitShiftLeft(toUInt128(1), 127), 128); -- { serverError PARAMETER_OUT_OF_BOUND }
SELECT bitTestAll(bitShiftLeft(toUInt256(1), 255), 256); -- { serverError PARAMETER_OUT_OF_BOUND }
SELECT bitTestAny(toInt256(-1), 256); -- { serverError PARAMETER_OUT_OF_BOUND }
SELECT bitTest(toUInt256(1), bitShiftLeft(toUInt128(1), 64)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT bitTest(toUInt256(1), bitShiftLeft(toUInt256(1), 128)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT bitTest(toUInt256(1), toInt128(0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT bitTest(toUInt256(1), toInt256(0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT bitTest(toUInt256(1), toInt8(-1)); -- { serverError PARAMETER_OUT_OF_BOUND }
SELECT bitTestAll(toUInt128(1), number) FROM numbers(129); -- { serverError PARAMETER_OUT_OF_BOUND }
SELECT bitTestAny(toUInt256(1), number) FROM numbers(257); -- { serverError PARAMETER_OUT_OF_BOUND }

-- Wide position types are rejected even when their values are valid.
SELECT bitTestAll(toUInt128(1), toUInt128(0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT bitTestAny(toUInt128(1), toUInt128(0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT bitTestAll(toUInt256(1), toUInt256(0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT bitTestAny(toUInt256(1), toUInt256(0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Wide position types are rejected at the type-checking stage for high valid values too.
SELECT bitTestAll(bitShiftLeft(toUInt128(1), 127), toUInt128(127)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT bitTestAny(bitShiftLeft(toUInt128(1), 127), toUInt128(127)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT bitTestAll(bitShiftLeft(toUInt256(1), 255), toUInt256(255)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT bitTestAny(bitShiftLeft(toUInt256(1), 255), toUInt256(255)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
