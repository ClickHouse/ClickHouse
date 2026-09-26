-- A constant haystack must give the same result as a column.

SELECT 'Kelvin sign', positionCaseInsensitiveUTF8(unhex('E284AA'), 'k'), positionCaseInsensitiveUTF8(materialize(unhex('E284AA')), 'k');
SELECT 'Ⱥ and ⱥ', positionCaseInsensitiveUTF8('Ⱥ', 'ⱥ'), positionCaseInsensitiveUTF8(materialize('Ⱥ'), 'ⱥ');
SELECT 'ASCII', positionCaseInsensitiveUTF8('xABCx', 'bc'), positionCaseInsensitiveUTF8(materialize('xABCx'), 'bc');
SELECT 'Cyrillic', positionCaseInsensitiveUTF8('ПРИВЕТ', 'вет'), positionCaseInsensitiveUTF8(materialize('ПРИВЕТ'), 'вет');
SELECT 'start position', positionCaseInsensitiveUTF8('abcABC', 'abc', 2), positionCaseInsensitiveUTF8(materialize('abcABC'), 'abc', 2);
SELECT 'Kelvin sign before k', positionCaseInsensitiveUTF8(concat('a', unhex('E284AA'), 'k'), 'k'), positionCaseInsensitiveUTF8(materialize(concat('a', unhex('E284AA'), 'k')), 'k');
