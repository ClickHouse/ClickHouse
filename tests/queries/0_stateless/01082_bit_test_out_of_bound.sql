SELECT '-- bitTestAny';
SELECT number, bitTestAny(toUInt8(1 + 4 + 16 + 64), number) FROM numbers(8);
SELECT number, bitTestAny(toUInt8(1 + 4 + 16 + 64), number) FROM numbers(8, 16); -- { serverError PARAMETER_OUT_OF_BOUND }

SELECT '-- bitTestAll';
SELECT number, bitTestAll(toUInt8(1 + 4 + 16 + 64), number) FROM numbers(8);
SELECT number, bitTestAll(toUInt8(1 + 4 + 16 + 64), number) FROM numbers(8, 16); -- { serverError PARAMETER_OUT_OF_BOUND }

SELECT '-- bitTest';
SELECT number, bitTest(toUInt8(1 + 4 + 16 + 64), number) FROM numbers(8);
SELECT number, bitTest(toUInt8(1 + 4 + 16 + 64), number) FROM numbers(8, 16); -- { serverError PARAMETER_OUT_OF_BOUND }
SELECT number, bitTest(toUInt16(1 + 4 + 16 + 64 + 256 + 1024 + 4096 + 16384 + 65536), number) FROM numbers(16);
SELECT -number, bitTest(toUInt16(1), -number) FROM numbers(8); -- { serverError PARAMETER_OUT_OF_BOUND }
