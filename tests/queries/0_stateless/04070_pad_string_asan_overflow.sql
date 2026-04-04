-- Regression test for heap-buffer-overflow in leftPad/rightPad.

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
