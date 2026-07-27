-- 11 MB haystack with 1M matches. `materialize` is required: the hang is in the
-- non-constant haystack code path.
SELECT countSubstringsCaseInsensitiveUTF8(materialize(repeat('\0\0\0\0\0\0\0\0\0\0', 1000000)), 'a');
SELECT countSubstringsCaseInsensitiveUTF8(materialize(repeat('\0\0\0\0\0\0\0\0\0\0a', 1000000)), 'a');
-- Many matches spread over several rows, with a start position, to keep the per-row cursor honest.
SELECT countSubstringsCaseInsensitiveUTF8(materialize(repeat('Я\0\0\0\0\0\0\0\0\0', number + 100000)), 'я', materialize(number + 2)) FROM numbers(4);
