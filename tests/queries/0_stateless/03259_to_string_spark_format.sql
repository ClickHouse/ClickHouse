SELECT '-- array format --';
SELECT CAST(array('\'1\'') , 'String') SETTINGS composed_data_type_output_format_mode = 'spark';
SELECT CAST([materialize('1'), '2', 'abc', '\'1\''], 'String') SETTINGS composed_data_type_output_format_mode = 'spark';
SELECT CAST([materialize('1'), materialize('2'), 'abc', '\'1\''], 'String') SETTINGS composed_data_type_output_format_mode = 'spark';
SELECT CAST([materialize('1'), materialize('2'), materialize('abc'), '\'1\''], 'String') SETTINGS composed_data_type_output_format_mode = 'spark';
SELECT CAST([materialize('1'), materialize('2'), materialize('abc'), materialize('\'1\'')], 'String') SETTINGS composed_data_type_output_format_mode = 'spark';

SELECT '-- map format --';
SELECT toString(map('1343', 'fe', 'afe', 'fefe')) SETTINGS composed_data_type_output_format_mode = 'spark';
SELECT toString(map(materialize('1343'), materialize('fe'), 'afe', 'fefe')) SETTINGS composed_data_type_output_format_mode = 'spark';
SELECT toString(map(materialize('1343'), materialize('fe'), materialize('afe'), 'fefe')) SETTINGS composed_data_type_output_format_mode = 'spark';
SELECT toString(map(materialize('1343'), materialize('fe'), materialize('afe'), materialize('fefe'))) SETTINGS composed_data_type_output_format_mode = 'spark';

SELECT '-- tuple format --';
SELECT toString(('1', '3', 'abc')) SETTINGS composed_data_type_output_format_mode = 'spark';
SELECT toString((materialize('1'), '3', 'abc')) SETTINGS composed_data_type_output_format_mode = 'spark';
SELECT toString((materialize('1'), materialize('3'), 'abc')) SETTINGS composed_data_type_output_format_mode = 'spark';
SELECT toString((materialize('1'), materialize('3'), materialize('abc'))) SETTINGS composed_data_type_output_format_mode = 'spark';
