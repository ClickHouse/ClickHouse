SELECT number, bitTestAny(toUInt8(1 + 4 + 16 + 64), number) FROM numbers(100);
SELECT number, bitTestAll(toUInt8(1 + 4 + 16 + 64), number) FROM numbers(100);
