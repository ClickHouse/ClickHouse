-- Tests that stringToH3 matches glibc sscanf("%" PRIx64) semantics for
-- "0x"/"0X" prefixes that are not followed by any hex digit.
-- glibc sscanf returns success with value 0 in those cases; the H3 backend
-- must do the same for backward compatibility.

SELECT stringToH3('0x');
SELECT stringToH3('0X');
SELECT stringToH3('0xg');
SELECT stringToH3('-0x');
SELECT stringToH3('+0x');

-- Sanity: a real hex value still parses.
SELECT stringToH3('0x89184926cc3ffff');
SELECT stringToH3('89184926cc3ffff');

-- The no-hex-digit parser-error path is swallowed and returns 0 (it does not
-- throw). On the old C-library backend these inputs threw; the sscanf-style
-- backend treats them as a silent 0 instead.
SELECT stringToH3('xyz');
SELECT stringToH3('');
SELECT stringToH3('   ');
SELECT stringToH3('-z');

-- The longest valid hex prefix is consumed and trailing garbage is ignored, so
-- a string with at least one leading hex digit parses to a non-zero value.
SELECT stringToH3('foo');

-- A successfully parsed zero value also returns 0 (no throw).
SELECT stringToH3('0');
SELECT stringToH3('000');
