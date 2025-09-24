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

DROP TABLE IF EXISTS tab;

CREATE TABLE tab(S1 String, S2 String, S3 FixedString(4)) ENGINE=Memory;
INSERT INTO tab values ('1a', 'a', 'AbA'), ('22', 'A', 'ab'), ('中国', '中', '国');

SELECT '-- Test constant needle with haystack - prefix';
SELECT COUNT() FROM tab WHERE startsWithCaseInsensitive(S1, '1');
SELECT COUNT() FROM tab WHERE startsWithCaseInsensitive(S2, '中');
SELECT COUNT() FROM tab WHERE startsWithCaseInsensitive(S3, '国');

SELECT COUNT() FROM tab WHERE startsWithUTF8CaseInsensitive(S1, '1');
SELECT COUNT() FROM tab WHERE startsWithUTF8CaseInsensitive(S2, '中');
-- startsWithCaseUTF8CaseInsensitive does not support FixedString

SELECT '-- Test constant needle with haystack - suffix';
SELECT COUNT() FROM tab WHERE endsWithCaseInsensitive(S1, '2');
SELECT COUNT() FROM tab WHERE endsWithCaseInsensitive(S2, '中');
SELECT COUNT() FROM tab WHERE endsWithCaseInsensitive(S3, '国\0');

SELECT COUNT() FROM tab WHERE endsWithUTF8CaseInsensitive(S1, '2');
SELECT COUNT() FROM tab WHERE endsWithUTF8CaseInsensitive(S2, '中');
-- endsWithCaseUTF8CaseInsensitive does not support FixedString

SELECT '-- Test column needle with haystack - prefix';
SELECT COUNT() FROM tab WHERE startsWithCaseInsensitive(S1, S1);
SELECT COUNT() FROM tab WHERE startsWithCaseInsensitive(S1, S2);
SELECT COUNT() FROM tab WHERE startsWithCaseInsensitive(S2, S3);

SELECT COUNT() FROM tab WHERE startsWithUTF8CaseInsensitive(S1, S1);
SELECT COUNT() FROM tab WHERE startsWithUTF8CaseInsensitive(S1, S2);
-- endsWithCaseUTF8CaseInsensitive does not support FixedString

SELECT '-- Test column needle with haystack - suffix';
SELECT COUNT() FROM tab WHERE endsWithCaseInsensitive(S1, S1);
SELECT COUNT() FROM tab WHERE endsWithCaseInsensitive(S1, S2);
SELECT COUNT() FROM tab WHERE endsWithCaseInsensitive(S2, S3);

SELECT COUNT() FROM tab WHERE endsWithUTF8CaseInsensitive(S1, S1);
SELECT COUNT() FROM tab WHERE endsWithUTF8CaseInsensitive(S1, S2);
-- endsWithCaseUTF8CaseInsensitive does not support FixedString

DROP TABLE tab;
