SELECT 'aaaabb ' == trim(leading 'b ' FROM 'b aaaabb ') x;
SELECT 'b aaaa' == trim(trailing 'b ' FROM 'b aaaabb ') x;
SELECT 'aaaa' == trim(both 'b ' FROM 'b aaaabb ') x;

SELECT '1' == replaceRegexpAll(',,1,,', '^[,]*|[,]*$', '') x;
SELECT '1' == replaceRegexpAll(',,1', '^[,]*|[,]*$', '') x;
SELECT '1' == replaceRegexpAll('1,,', '^[,]*|[,]*$', '') x;

SELECT '1,,' == replaceRegexpOne(',,1,,', '^[,]*|[,]*$', '') x;
SELECT '1' == replaceRegexpOne(',,1', '^[,]*|[,]*$', '') x;
SELECT '1,,' == replaceRegexpOne('1,,', '^[,]*|[,]*$', '') x;

SELECT '5935,5998,6014' == trim(BOTH ', ' FROM '5935,5998,6014, ') x;
SELECT '5935,5998,6014' == replaceRegexpAll('5935,5998,6014, ', concat('^[', regexpQuoteMeta(', '), ']*|[', regexpQuoteMeta(', '), ']*$'), '') AS x;

SELECT trim(BOTH '"' FROM '2') == '2'
