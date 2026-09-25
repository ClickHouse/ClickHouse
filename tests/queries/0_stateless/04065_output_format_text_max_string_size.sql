SELECT 'abcdef' SETTINGS output_format_text_max_string_size = 3 FORMAT TabSeparatedRaw;
SELECT 'abcdef' AS s SETTINGS output_format_text_max_string_size = 3 FORMAT JSONEachRow;
SELECT toFixedString('abcdef', 6) SETTINGS output_format_text_max_string_size = 3 FORMAT TabSeparatedRaw;
SELECT toFixedString('abcdef', 6) AS s SETTINGS output_format_text_max_string_size = 3 FORMAT JSONEachRow;
SELECT ['abcdef', 'ghijkl'] AS arr SETTINGS output_format_text_max_string_size = 3 FORMAT JSONEachRow;
SELECT map('abcdef', 'ghijkl') AS m SETTINGS output_format_text_max_string_size = 3 FORMAT JSONEachRow;
SELECT 'abcdef' SETTINGS output_format_text_max_string_size = 0 FORMAT TabSeparatedRaw;
