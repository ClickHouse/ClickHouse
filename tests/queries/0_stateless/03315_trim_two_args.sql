-- Tests the second argument (custom trim characters) for functions trim, trimLeft and trimRight.

SELECT 'Basic custom character trimming';
SELECT
    trimLeft('#@hello#@', '#@') = 'hello#@' AS left_custom_ok,
    trimRight('#@hello#@', '#@') = '#@hello' AS right_custom_ok,
    trimBoth('#@hello#@', '#@') = 'hello' AS both_custom_ok,
    trimLeft(toFixedString('#@hello#@', 9), '#@') = 'hello#@' AS left_fixed_custom_ok,
    trimRight(toFixedString('#@hello#@', 9), '#@') = '#@hello' AS right_fixed_custom_ok,
    trimBoth(toFixedString('#@hello#@', 9), '#@') = 'hello' AS both_fixed_custom_ok;

SELECT 'Same AS before but with non-const input strings';
SELECT
    trimLeft(materialize('#@hello#@'), '#@') = 'hello#@' AS left_custom_ok,
    trimRight(materialize('#@hello#@'), '#@') = '#@hello' AS right_custom_ok,
    trimBoth(materialize('#@hello#@'), '#@') = 'hello' AS both_custom_ok,
    trimLeft(materialize(toFixedString('#@hello#@', 9)), '#@') = 'hello#@' AS left_fixed_custom_ok,
    trimRight(materialize(toFixedString('#@hello#@', 9)), '#@') = '#@hello' AS right_fixed_custom_ok,
    trimBoth(materialize(toFixedString('#@hello#@', 9)), '#@') = 'hello' AS both_fixed_custom_ok;

SELECT 'Multiple different characters to trim';
SELECT
    trimLeft('##@@hello##@@', '#@') = 'hello##@@' AS left_multi_ok,
    trimRight('##@@hello##@@', '#@') = '##@@hello' AS right_multi_ok,
    trimBoth('##@@hello##@@', '#@') = 'hello' AS both_multi_ok,
    trimLeft(toFixedString('##@@hello##@@', 13), '#@') = 'hello##@@' AS left_fixed_multi_ok,
    trimRight(toFixedString('##@@hello##@@', 13), '#@') = '##@@hello' AS right_fixed_multi_ok,
    trimBoth(toFixedString('##@@hello##@@', 13), '#@') = 'hello' AS both_fixed_multi_ok;

SELECT 'Empty trim character string';
SELECT
    trimLeft('  hello  ', '') = '  hello  ' AS left_empty_chars_ok,
    trimRight('  hello  ', '') = '  hello  ' AS right_empty_chars_ok,
    trimBoth('  hello  ', '') = '  hello  ' AS both_empty_chars_ok,
    trimLeft(toFixedString('  hello  ', 9), '') = '  hello  ' AS left_fixed_empty_chars_ok,
    trimRight(toFixedString('  hello  ', 9), '') = '  hello  ' AS right_fixed_empty_chars_ok,
    trimBoth(toFixedString('  hello  ', 9), '') = '  hello  ' AS both_fixed_empty_chars_ok;

SELECT 'Empty string to trim';
SELECT
    trimLeft('', '#@') = '' AS left_empty_str_ok,
    trimRight('', '#@') = '' AS right_empty_str_ok,
    trimBoth('', '#@') = '' AS both_empty_str_ok;
    -- FixedString(0) is illegal --> no tests


SELECT 'String containing only trim characters';
SELECT
    trimLeft('####', '#') = '' AS left_only_trim_chars_ok,
    trimRight('####', '#') = '' AS right_only_trim_chars_ok,
    trimBoth('####', '#') = '' AS both_only_trim_chars_ok,
    trimLeft(toFixedString('####', 4), '#') = '' AS left_fixed_only_trim_chars_ok,
    trimRight(toFixedString('####', 4), '#') = '' AS right_fixed_only_trim_chars_ok,
    trimBoth(toFixedString('####', 4), '#') = '' AS both_fixed_only_trim_chars_ok;

SELECT 'Characters that have special meaning in regex';
SELECT
    trimLeft('...hello...', '.') = 'hello...' AS left_special_ok,
    trimRight('...hello...', '.') = '...hello' AS right_special_ok,
    trimBoth('...hello...', '.') = 'hello' AS both_special_ok,
    trimLeft(toFixedString('...hello...', 11), '.') = 'hello...' AS left_fixed_special_ok,
    trimRight(toFixedString('...hello...', 11), '.') = '...hello' AS right_fixed_special_ok,
    trimBoth(toFixedString('...hello...', 11), '.') = 'hello' AS both_fixed_special_ok;

SELECT 'Very long input strings';
WITH
    repeat('x', 1000) AS long_str,
    toFixedString(long_str, 1000) AS long_fixed_str,
    repeat('#@', 50) AS trim_chars
SELECT
    length(trimLeft(concat(trim_chars, long_str, trim_chars), '#@')) = 1100 AS left_long_ok,
    length(trimRight(concat(trim_chars, long_str, trim_chars), '#@')) = 1100 AS right_long_ok,
    length(trimBoth(concat(trim_chars, long_str, trim_chars), '#@')) = 1000 AS both_long_ok,
    length(trimLeft(concat(trim_chars, long_fixed_str, trim_chars), '#@')) = 1100 AS left_fixed_long_ok,
    length(trimRight(concat(trim_chars, long_fixed_str, trim_chars), '#@')) = 1100 AS right_fixed_long_ok,
    length(trimBoth(concat(trim_chars, long_fixed_str, trim_chars), '#@')) = 1000 AS both_fixed_long_ok;

SELECT 'Overlapping trim characters';
SELECT
    trimLeft('aabbccHELLOccbbaa', 'abc') = 'HELLOccbbaa' AS left_overlap_ok,
    trimRight('aabbccHELLOccbbaa', 'abc') = 'aabbccHELLO' AS right_overlap_ok,
    trimBoth('aabbccHELLOccbbaa', 'abc') = 'HELLO' AS both_overlap_ok,
    trimLeft(toFixedString('aabbccHELLOccbbaa', 17), 'abc') = 'HELLOccbbaa' AS left_fixed_overlap_ok,
    trimRight(toFixedString('aabbccHELLOccbbaa', 17), 'abc') = 'aabbccHELLO' AS right_fixed_overlap_ok,
    trimBoth(toFixedString('aabbccHELLOccbbaa', 17), 'abc') = 'HELLO' AS both_fixed_overlap_ok;

SELECT 'Same trim characters provided more than once';
SELECT
    trimLeft('#@hello#@', '#@#@') = 'hello#@' AS left_custom_ok,
    trimRight('#@hello#@', '#@#@') = '#@hello' AS right_custom_ok,
    trimBoth('#@hello#@', '#@#@') = 'hello' AS both_custom_ok,
    trimLeft(toFixedString('#@hello#@', 9), '#@#@') = 'hello#@' AS left_fixed_custom_ok,
    trimRight(toFixedString('#@hello#@', 9), '#@#@') = '#@hello' AS right_fixed_custom_ok,
    trimBoth(toFixedString('#@hello#@', 9), '#@#@') = 'hello' AS both_fixed_custom_ok;

SELECT 'Negative tests';
SELECT trimLeft('hello', 'a', 'b'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT trimRight(123, 'a'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT trimBoth('hello', 123); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT trimBoth('hello', materialize('a')); -- { serverError ILLEGAL_COLUMN }

SELECT 'Special tests';

CREATE TABLE tab (col FixedString(3)) ENGINE = Memory;
INSERT INTO tab VALUES ('abc');
SELECT trim(trailing char(0) from col) FROM tab;
SELECT trim(both 'ac' from col) FROM tab;
DROP TABLE tab;

-- Bug 78796
SELECT isConstant(trimBoth(''));
