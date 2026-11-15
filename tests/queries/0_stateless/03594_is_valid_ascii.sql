SELECT 1 = isValidASCII('') FROM system.numbers LIMIT 1;
SELECT 1 = isValidASCII('some text') FROM system.numbers LIMIT 1;
SELECT 1 = isValidASCII('\x00') FROM system.numbers LIMIT 1;
SELECT 1 = isValidASCII('\x66') FROM system.numbers LIMIT 1;
SELECT 1 = isValidASCII('\x7F') FROM system.numbers LIMIT 1;
SELECT 1 = isValidASCII('\x00\x7F') FROM system.numbers LIMIT 1;
SELECT 1 = isValidASCII('\x7F\x00') FROM system.numbers LIMIT 1;
SELECT 0 = isValidASCII('какой-то текст') FROM system.numbers LIMIT 1;
SELECT 0 = isValidASCII('\xC2\x80') FROM system.numbers LIMIT 1;
SELECT 1 = isValidASCII('hello world!') FROM system.numbers LIMIT 1;

SELECT 1 = isASCII('') FROM system.numbers LIMIT 1;
SELECT 1 = isASCII('some text') FROM system.numbers LIMIT 1;
SELECT 1 = isASCII('\x00') FROM system.numbers LIMIT 1;
SELECT 1 = isASCII('\x66') FROM system.numbers LIMIT 1;
SELECT 1 = isASCII('\x7F') FROM system.numbers LIMIT 1;
SELECT 1 = isASCII('\x00\x7F') FROM system.numbers LIMIT 1;
SELECT 1 = isASCII('\x7F\x00') FROM system.numbers LIMIT 1;
SELECT 0 = isASCII('какой-то текст') FROM system.numbers LIMIT 1;
SELECT 0 = isASCII('\xC2\x80') FROM system.numbers LIMIT 1;
SELECT 1 = isASCII('hello world!') FROM system.numbers LIMIT 1;

SELECT isValidASCII(toString(number)) FROM system.numbers WHERE number < 10;

SELECT 1 = isValidASCII('\x00') FROM system.numbers LIMIT 1;
SELECT 1 = isValidASCII('\x7F') FROM system.numbers LIMIT 1;
SELECT 0 = isValidASCII('\x80') FROM system.numbers LIMIT 1;
SELECT 0 = isValidASCII('\xFF') FROM system.numbers LIMIT 1;

SELECT 0 = isValidASCII('Hello\x80World') FROM system.numbers LIMIT 1;
SELECT 0 = isValidASCII('ASCII\xC2\x80Text') FROM system.numbers LIMIT 1;
SELECT 1 = isValidASCII('Pure ASCII 123 !@#') FROM system.numbers LIMIT 1;

SELECT 1 = isValidASCII(toFixedString('ASCII', 5)) FROM system.numbers LIMIT 1;
SELECT 0 = isValidASCII(toFixedString('ASCII\x80', 6)) FROM system.numbers LIMIT 1; 