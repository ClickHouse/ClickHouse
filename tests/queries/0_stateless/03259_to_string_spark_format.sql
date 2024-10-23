SELECT '-- array format';
SELECT CAST(array('\'1\'') , 'String') SETTINGS spark_text_output_format=1;
SELECT CAST([materialize('1'), '2', 'abc', '\'1\''], 'String') SETTINGS spark_text_output_format = 1;
SELECT CAST([materialize('1'), materialize('2'), 'abc', '\'1\''], 'String') SETTINGS spark_text_output_format = 1;
SELECT CAST([materialize('1'), materialize('2'), materialize('abc'), '\'1\''], 'String') SETTINGS spark_text_output_format = 1;
SELECT CAST([materialize('1'), materialize('2'), materialize('abc'), materialize('\'1\'')], 'String') SETTINGS spark_text_output_format = 1;

SELECT '-- map format';
SELECT toString(map('1343', 'fe', 'afe', 'fefe')) SETTINGS spark_text_output_format = 1;
SELECT toString(map(materialize('1343'), materialize('fe'), 'afe', 'fefe')) SETTINGS spark_text_output_format = 1;
SELECT toString(map(materialize('1343'), materialize('fe'), materialize('afe'), 'fefe')) SETTINGS spark_text_output_format = 1;
SELECT toString(map(materialize('1343'), materialize('fe'), materialize('afe'), materialize('fefe'))) SETTINGS spark_text_output_format = 1;

SELECT '-- tuple format';
SELECT toString(('1', '3', 'abc')) SETTINGS spark_text_output_format = 1;
SELECT toString((materialize('1'), '3', 'abc')) SETTINGS spark_text_output_format = 1;
SELECT toString((materialize('1'), materialize('3'), 'abc')) SETTINGS spark_text_output_format = 1;
SELECT toString((materialize('1'), materialize('3'), materialize('abc'))) SETTINGS spark_text_output_format = 1;
