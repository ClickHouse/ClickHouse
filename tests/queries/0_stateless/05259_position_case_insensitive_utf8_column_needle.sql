-- A needle column must give the same result as a constant needle.
-- The lowercase of 'Ⱥ' (2 bytes) is 'ⱥ' (3 bytes), so the needle does not fit in the haystack.

SELECT 'Ⱥ and ⱥ', positionCaseInsensitiveUTF8(materialize('Ⱥ'), 'ⱥ'), positionCaseInsensitiveUTF8(materialize('Ⱥ'), materialize('ⱥ')), positionCaseInsensitiveUTF8('Ⱥ', materialize('ⱥ'));
SELECT 'ⱥ at the end', positionCaseInsensitiveUTF8(materialize('abȺ'), 'ⱥ'), positionCaseInsensitiveUTF8(materialize('abȺ'), materialize('ⱥ')), positionCaseInsensitiveUTF8('abȺ', materialize('ⱥ'));
SELECT 'ⱥ in the middle', positionCaseInsensitiveUTF8(materialize('aȺbc'), 'ⱥ'), positionCaseInsensitiveUTF8(materialize('aȺbc'), materialize('ⱥ')), positionCaseInsensitiveUTF8('aȺbc', materialize('ⱥ'));
SELECT 'ASCII', positionCaseInsensitiveUTF8(materialize('xABCx'), 'bc'), positionCaseInsensitiveUTF8(materialize('xABCx'), materialize('bc')), positionCaseInsensitiveUTF8('xABCx', materialize('bc'));
SELECT 'start position', positionCaseInsensitiveUTF8(materialize('abcABC'), 'abc', 2), positionCaseInsensitiveUTF8(materialize('abcABC'), materialize('abc'), 2), positionCaseInsensitiveUTF8('abcABC', materialize('abc'), 2);
