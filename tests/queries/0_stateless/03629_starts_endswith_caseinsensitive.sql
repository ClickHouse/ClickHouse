-- Test for case-insensitive prefix and suffix matching functions, (starts|endswith)(UTF8)?CaseInsensitive

SELECT '-- Test startsWithCaseInsensitive ASCII';
SELECT startsWithCaseInsensitive('Marta', 'mA'), startsWithCaseInsensitive('match_me_not', 'Na');

SELECT '-- Test startsWithCaseInsensitive UTF8 Latin';
SELECT startsWithCaseInsensitive('Bär', 'bä'), startsWithCaseInsensitive('Bär', 'BÄ');

SELECT '-- Test startsWithCaseInsensitive UTF8 non-Latin';
SELECT startsWithCaseInsensitive('中国', '中'), startsWithCaseInsensitive('中国', '国');

SELECT '-- Test startsWithUTF8CaseInsensitive ASCII';
SELECT startsWithUTF8CaseInsensitive('Marta', 'mA'), startsWithUTF8CaseInsensitive('match_me_not', 'Na');

SELECT '-- Test startsWithUTF8CaseInsensitive UTF8 Latin';
SELECT startsWithUTF8CaseInsensitive('Bär', 'bä'), startsWithUTF8CaseInsensitive('Bär', 'BÄ');

SELECT '-- Test startsWithUTF8CaseInsensitive UTF8 non-Latin';
SELECT startsWithUTF8CaseInsensitive('中国', '中'), startsWithUTF8CaseInsensitive('Hello中国', '中');

SELECT '-- Test endsWithCaseInsensitive ASCII';
SELECT endsWithCaseInsensitive('Marta', 'tA'), endsWithCaseInsensitive('match_me_not', 'Na');

SELECT '-- Test endsWithCaseInsensitive UTF8 Latin';
SELECT endsWithCaseInsensitive('Bär', 'äR'), endsWithCaseInsensitive('Bär', 'ÄR');

SELECT '-- Test endsWithCaseInsensitive UTF8 non-Latin';
SELECT endsWithCaseInsensitive('中国', '国'), endsWithCaseInsensitive('中国', '中');

SELECT '-- Test endsWithUTF8CaseInsensitive ASCII';
SELECT endsWithUTF8CaseInsensitive('Marta', 'tA'), endsWithUTF8CaseInsensitive('match_me_not', 'Na');

SELECT '-- Test endsWithUTF8CaseInsensitive UTF8 Latin';
SELECT endsWithUTF8CaseInsensitive('Bär', 'äR'), endsWithUTF8CaseInsensitive('Bär', 'ÄR');

SELECT '-- Test endsWithUTF8CaseInsensitive UTF8 non-Latin';
SELECT endsWithUTF8CaseInsensitive('中国', '国'), endsWithUTF8CaseInsensitive('中国', '中');

SELECT '-- Test invalid UTF8';
SELECT startsWithCaseInsensitive('中国', '\xe4'), startsWithUTF8CaseInsensitive('中国', '\xe4');
SELECT endsWithCaseInsensitive('中国', '\xbd'), endsWithUTF8CaseInsensitive('中国', '\xbd');
