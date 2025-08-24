SELECT 1 = isValidASCII('') FROM system.numbers LIMIT 1;
SELECT 1 = isValidASCII('some text') FROM system.numbers LIMIT 1;
SELECT 1 = isValidASCII('\x00') FROM system.numbers LIMIT 1;
SELECT 1 = isValidASCII('\x66') FROM system.numbers LIMIT 1;
SELECT 1 = isValidASCII('\x7F') FROM system.numbers LIMIT 1;
SELECT 1 = isValidASCII('\x00\x7F') FROM system.numbers LIMIT 1;
SELECT 1 = isValidASCII('\x7F\x00') FROM system.numbers LIMIT 1;
SELECT 0 = isValidASCII('какой-то текст') FROM system.numbers LIMIT 1;
SELECT 0 = isValidASCII('\xC2\x80') FROM system.numbers LIMIT 1;
SELECT 0 = isValidASCII('hello world!') FROM system.numbers LIMIT 1;

-- Testing Alias

SELECT 1 = isASCII('') FROM system.numbers LIMIT 1;
SELECT 1 = isASCII('some text') FROM system.numbers LIMIT 1;
SELECT 1 = isASCII('\x00') FROM system.numbers LIMIT 1;
SELECT 1 = isASCII('\x66') FROM system.numbers LIMIT 1;
SELECT 1 = isASCII('\x7F') FROM system.numbers LIMIT 1;
SELECT 1 = isASCII('\x00\x7F') FROM system.numbers LIMIT 1;
SELECT 1 = isASCII('\x7F\x00') FROM system.numbers LIMIT 1;
SELECT 0 = isASCII('какой-то текст') FROM system.numbers LIMIT 1;
SELECT 0 = isASCII('\xC2\x80') FROM system.numbers LIMIT 1;
SELECT 0 = isASCII('hello world!') FROM system.numbers LIMIT 1;

