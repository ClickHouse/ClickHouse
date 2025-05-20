SET enable_analyzer = 1;

SELECT c1, c2, c3, c4 FROM format('CSV', '1,2,"[1,2,3]","[[\'abc\'], [], [\'d\', \'e\']]"');
SELECT f.c1, f.c2, f.c3, f.c4 FROM format('CSV', '1,2,"[1,2,3]","[[\'abc\'], [], [\'d\', \'e\']]"') AS f;
SELECT f.* FROM format('CSV', '1,2,"[1,2,3]","[[\'abc\'], [], [\'d\', \'e\']]"') AS f;

WITH 'CSV', '1,2,"[1,2,3]","[[\'abc\'], [], [\'d\', \'e\']]"' AS format_value SELECT c1, c2, c3, c4 FROM format('CSV', format_value);
WITH concat('1,2,"[1,2,3]",','"[[\'abc\'], [], [\'d\', \'e\']]"') AS format_value SELECT c1, c2, c3, c4 FROM format('CSV', format_value);

SELECT format, format_value, c1, c2, c3, c4 FROM format('CSV' AS format, '1,2,"[1,2,3]","[[\'abc\'], [], [\'d\', \'e\']]"' AS format_value);
