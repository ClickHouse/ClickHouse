-- 11 MB haystack with 1M matches. `materialize` is required: the hang is in the
-- non-constant haystack code path.
SELECT countSubstringsCaseInsensitiveUTF8(materialize(repeat('\0\0\0\0\0\0\0\0\0\0', 1000000)), 'a');
SELECT countSubstringsCaseInsensitiveUTF8(materialize(repeat('\0\0\0\0\0\0\0\0\0\0a', 1000000)), 'a');
-- Many matches spread over several rows, with a start position, to keep the per-row cursor honest.
SELECT countSubstringsCaseInsensitiveUTF8(materialize(repeat('Я\0\0\0\0\0\0\0\0\0', number + 100000)), 'я', materialize(number + 2)) FROM numbers(4);
-- The offset of a match is a character offset accumulated over the whole row, so it must not be
-- a byte offset and must not restart at the previous match. Multibyte characters sit before the
-- matches, and the start position is past the distance between two matches.
SELECT countSubstringsCaseInsensitiveUTF8(materialize('ЯaЯaЯa'), 'a', 5);
-- Same, across a row boundary: the cursor must restart at the beginning of the second row.
SELECT countSubstringsCaseInsensitiveUTF8(materialize('ЯaЯaЯa'), 'a', materialize(1 + number * 5)) FROM numbers(2);
