-- Test basic custom character trimming
SELECT
    trimLeft('#@hello#@', '#@') = 'hello#@' as left_custom_ok,
    trimRight('#@hello#@', '#@') = '#@hello' as right_custom_ok,
    trimBoth('#@hello#@', '#@') = 'hello' as both_custom_ok;

-- Test with multiple different characters to trim
SELECT
    trimLeft('##@@hello##@@', '#@') = 'hello##@@' as left_multi_ok,
    trimRight('##@@hello##@@', '#@') = '##@@hello' as right_multi_ok,
    trimBoth('##@@hello##@@', '#@') = 'hello' as both_multi_ok;

-- Test with empty trim character string
SELECT
    trimLeft('  hello  ', '') = '  hello  ' as left_empty_chars_ok,
    trimRight('  hello  ', '') = '  hello  ' as right_empty_chars_ok,
    trimBoth('  hello  ', '') = '  hello  ' as both_empty_chars_ok;

-- Test with empty string to trim
SELECT
    trimLeft('', '#@') = '' as left_empty_str_ok,
    trimRight('', '#@') = '' as right_empty_str_ok,
    trimBoth('', '#@') = '' as both_empty_str_ok;

-- Test with string containing only trim characters
SELECT
    trimLeft('####', '#') = '' as left_only_trim_chars_ok,
    trimRight('####', '#') = '' as right_only_trim_chars_ok,
    trimBoth('####', '#') = '' as both_only_trim_chars_ok;

-- Test with characters that have special meaning in regex
SELECT
    trimLeft('...hello...', '.') = 'hello...' as left_special_ok,
    trimRight('...hello...', '.') = '...hello' as right_special_ok,
    trimBoth('...hello...', '.') = 'hello' as both_special_ok;

-- Test with very long strings and multiple trim characters
WITH
    repeat('x', 1000) as long_str,
    repeat('#@', 50) as trim_chars
SELECT
    length(trimLeft(concat(trim_chars, long_str, trim_chars), '#@')) = 1100 as left_long_ok,
    length(trimRight(concat(trim_chars, long_str, trim_chars), '#@')) = 1100 as right_long_ok,
    length(trimBoth(concat(trim_chars, long_str, trim_chars), '#@')) = 1000 as both_long_ok;

-- Test with overlapping trim characters
SELECT
    trimLeft('aabbccHELLOccbbaa', 'abc') = 'HELLOccbbaa' as left_overlap_ok,
    trimRight('aabbccHELLOccbbaa', 'abc') = 'aabbccHELLO' as right_overlap_ok,
    trimBoth('aabbccHELLOccbbaa', 'abc') = 'HELLO' as both_overlap_ok;

-- Test error cases for two-argument version
SELECT trimLeft('hello', 'a', 'b'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT trimRight(123, 'a'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT trimBoth('hello', 123); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
