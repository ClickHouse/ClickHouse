-- Tags: no-fasttest, no-cpu-aarch64

SELECT reinterpretAsUInt128(aquaHash128(123456));
SELECT reinterpretAsUInt128(aquaHash128(123, 456));
SELECT reinterpretAsUInt128(aquaHash128('abc'));