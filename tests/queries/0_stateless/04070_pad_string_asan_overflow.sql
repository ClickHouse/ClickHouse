-- Regression test for heap-buffer-overflow in leftPad/rightPad.
-- writeSlice() uses memcpySmallAllowReadWriteOverflow15 which reads in 16-byte
-- SIMD chunks and may overread up to 15 bytes past the source buffer. The pad
-- string data must be stored in a PaddedPODArray to provide this read padding.
-- Without the fix, AddressSanitizer detects a heap-buffer-overflow here.

-- Pad strings of various lengths exercise different SIMD chunk counts:
-- 'abcdefghijklmnopq' (17 chars, no doubling, worst case: 15 byte overread)
-- 'abcdefghi' (9 chars, doubled to 18, 14 byte overread)
-- 'abc' (3 chars, doubled to 24, 8 byte overread)
-- 'abcdefghijklmnopqrstuvwxyz0123456' (33 chars, 3 SIMD chunks)

-- Non-UTF8 paths (PaddingChars<false>)
SELECT leftPad('x', 100, 'abcdefghijklmnopq');
SELECT rightPad('x', 100, 'abcdefghijklmnopq');
SELECT leftPad('x', 100, 'abcdefghi');
SELECT rightPad('x', 100, 'abcdefghi');
SELECT leftPad('x', 50, 'abc');
SELECT rightPad('x', 50, 'abc');
SELECT leftPad('x', 100, 'abcdefghijklmnopqrstuvwxyz0123456');
SELECT rightPad('x', 100, 'abcdefghijklmnopqrstuvwxyz0123456');

-- UTF8 paths (PaddingChars<true>) with multi-byte characters
SELECT leftPadUTF8('x', 50, 'абвгдежзиклмнопрс');
SELECT rightPadUTF8('x', 50, 'абвгдежзиклмнопрс');
SELECT leftPadUTF8('x', 50, 'αβγδε');
SELECT rightPadUTF8('x', 50, 'αβγδε');

-- Verify correctness with table data (non-const string column)
SELECT leftPad(s, 30, 'abcdefghijklmnopq') FROM (SELECT arrayJoin(['hello', 'world', 'test']) AS s);
SELECT rightPad(s, 30, 'abcdefghijklmnopq') FROM (SELECT arrayJoin(['hello', 'world', 'test']) AS s);
