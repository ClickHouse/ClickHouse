SELECT '-- array format --';
SELECT CAST(array('\'1\'') , 'String') SETTINGS composed_data_type_output_format_mode = 'spark';
SELECT CAST([materialize('1'), '2', 'abc', '\'1\''], 'String') SETTINGS composed_data_type_output_format_mode = 'spark';
SELECT CAST([materialize('1'), materialize('2'), 'abc', '\'1\''], 'String') SETTINGS composed_data_type_output_format_mode = 'spark';
SELECT CAST([materialize('1'), materialize('2'), materialize('abc'), '\'1\''], 'String') SETTINGS composed_data_type_output_format_mode = 'spark';
SELECT CAST([materialize('1'), materialize('2'), materialize('abc'), materialize('\'1\'')], 'String') SETTINGS composed_data_type_output_format_mode = 'spark';
SELECT toJSONString(array(1, 2, 3)) SETTINGS composed_data_type_output_format_mode = 'spark';
SELECT toJSONString(materialize(array(1, 2, 3))) from numbers(10) SETTINGS composed_data_type_output_format_mode = 'spark';
SELECT CAST(toString(materialize(array('1343', 'fe', 'afe', 'fefe'))), 'Array(String)') from numbers(5) SETTINGS composed_data_type_output_format_mode = 'spark'; -- { serverError CANNOT_PARSE_QUOTED_STRING }
SELECT CAST(toString(array('1343', 'fe', 'afe', 'fefe')), 'Array(String)') SETTINGS composed_data_type_output_format_mode = 'spark'; -- { serverError CANNOT_PARSE_QUOTED_STRING }

SELECT '-- map format --';
SELECT toString(map('1343', 'fe', 'afe', 'fefe')) SETTINGS composed_data_type_output_format_mode = 'spark';
SELECT toString(map(materialize('1343'), materialize('fe'), 'afe', 'fefe')) SETTINGS composed_data_type_output_format_mode = 'spark';
SELECT toString(map(materialize('1343'), materialize('fe'), materialize('afe'), 'fefe')) SETTINGS composed_data_type_output_format_mode = 'spark';
SELECT toString(map(materialize('1343'), materialize('fe'), materialize('afe'), materialize('fefe'))) SETTINGS composed_data_type_output_format_mode = 'spark';
SELECT toJSONString(map('key1', 1, 'key2', 2)) SETTINGS composed_data_type_output_format_mode = 'spark';
SELECT toJSONString(materialize(map('key1', 1, 'key2', 2))) from numbers(10) SETTINGS composed_data_type_output_format_mode = 'spark';

SELECT '-- tuple format --';
SELECT toString(('1', '3', 'abc')) SETTINGS composed_data_type_output_format_mode = 'spark';
SELECT toString((materialize('1'), '3', 'abc')) SETTINGS composed_data_type_output_format_mode = 'spark';
SELECT toString((materialize('1'), materialize('3'), 'abc')) SETTINGS composed_data_type_output_format_mode = 'spark';
SELECT toString((materialize('1'), materialize('3'), materialize('abc'))) SETTINGS composed_data_type_output_format_mode = 'spark';

SELECT toJSONString(tuple('key1', 1, 'key2', 2)) SETTINGS composed_data_type_output_format_mode = 'spark';
SELECT toJSONString(materialize(tuple('key1', 1, 'key2', 2))) from numbers(10) SETTINGS composed_data_type_output_format_mode = 'spark';
