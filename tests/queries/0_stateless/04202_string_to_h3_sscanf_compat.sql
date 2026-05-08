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
