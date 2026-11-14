SELECT name, age FROM format(CSVWithNames, '# This is a header comment\nname,age\n# Comment right after header\nAlice,30\n# Comment between rows\nBob,25\n# Comment at EOF', format_csv_comment_prefix = '#') ORDER BY name;

SELECT * FROM format(TSVWithNames, '#Code\tUCode\tStateCode\tType\n# Updated 2026 Oct 16 2334:11\nSAT1\tU1\tUS\tPAYLOAD\n# Another comment\nSAT2\tU2\tFR\tROCKET\n', format_tsv_comment_prefix = '# ') ORDER BY `#Code`;

SELECT toTypeName(col1), toTypeName(col2) FROM format(CSVWithNames, '# Comment\ncol1,col2\n# Comment after header\n1,hello\n2,world\n', input_format_csv_comment_prefix = '#') LIMIT 1;

SELECT toTypeName(id), toTypeName(val) FROM format(TSVWithNames, 'id\tval\n# Comment after header\n100\ttest\n', input_format_tsv_comment_prefix = '#') LIMIT 1;

SELECT * FROM format(CSV, '# Comment before row 1\n1,2\n# Comment between rows\n3,4\n# Comment at end', input_format_csv_comment_prefix = '#') ORDER BY c1;

SELECT * FROM format(TSV, '# Comment before row 1\n10\t20\n# Comment between rows\n30\t40\n# Comment at end', input_format_tsv_comment_prefix = '#') ORDER BY c1;

SELECT * FROM format(CSVWithNames, '// Header comment\nname,score\n// Comment after header\nJohn,95\n// Comment\nJane,99', format_csv_comment_prefix = '//') ORDER BY name;

SELECT count() FROM format(CSV, '#line1\n#line2\nline3');
SELECT count() FROM format(CSV, '#line1\n#line2\nline3', format_csv_comment_prefix = '#');
