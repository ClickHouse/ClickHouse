-- ASCII
SELECT leftPad('x', 100, 'abcdefghijklmnopq');
SELECT rightPad('x', 100, 'abcdefghijklmnopq');
SELECT leftPad('x', 100, 'abcdefghi');
SELECT rightPad('x', 100, 'abcdefghi');
SELECT leftPad('x', 50, 'abc');
SELECT rightPad('x', 50, 'abc');
SELECT leftPad('x', 100, 'abcdefghijklmnopqrstuvwxyz0123456');
SELECT rightPad('x', 100, 'abcdefghijklmnopqrstuvwxyz0123456');

-- UTF8
SELECT leftPadUTF8('x', 50, 'абвгдежзиклмнопрс');
SELECT rightPadUTF8('x', 50, 'абвгдежзиклмнопрс');
SELECT leftPadUTF8('x', 50, 'αβγδε');
SELECT rightPadUTF8('x', 50, 'αβγδε');

-- More tests with non-const data
SELECT leftPad(s, 30, 'abcdefghijklmnopq') FROM (SELECT arrayJoin(['hello', 'world', 'test']) AS s);
SELECT rightPad(s, 30, 'abcdefghijklmnopq') FROM (SELECT arrayJoin(['hello', 'world', 'test']) AS s);
