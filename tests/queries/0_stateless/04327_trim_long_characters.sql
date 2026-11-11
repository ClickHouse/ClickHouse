-- Custom trim character sets longer than 16 characters must be supported.
-- Regression: trim* threw TOO_LARGE_STRING_SIZE for sets exceeding the 16-symbol
-- SearchSymbols SIMD limit (https://github.com/ClickHouse/ClickHouse/pull/93543).

SELECT trimLeft('SRID=4326;POINT (1 2)', 'SRID=4326;POINT (');
SELECT trimRight('hello]]]}}})))', ')}]');
SELECT trimBoth('abcdefghijklmnopqrXYZxyz', 'abcdefghijklmnopqr');

-- A character beyond the 16th position must still participate in trimming.
SELECT trimLeft('XYZ123', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ');
SELECT trimRight('123XYZ', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ');
SELECT ltrim('0123456789abcdefgXYZ', '0123456789abcdefg');
SELECT rtrim('XYZ0123456789abcdefg', '0123456789abcdefg');

-- Default whitespace trimming is unaffected.
SELECT trimBoth('   spaces   ');

-- Works over a column too (set built once, applied per row).
SELECT trimLeft(s, 'SRID=4326;POINT (') FROM values('s String', 'SRID=4326;POINT (10 20)', 'SRID=4326;POINT (-1 -2)');
