-- Test for case-insensitive prefix and suffix matching functions, (starts|endswith)(UTF8)?CaseInsensitive

SELECT '-- Test startsWithCaseInsensitive ASCII';
SELECT startsWithCaseInsensitive('Marta', 'mA'), startsWithCaseInsensitive('match_me_not', 'Na');

SELECT '-- Test startsWithCaseInsensitive UTF8 Latin';
SELECT startsWithCaseInsensitive('Bär', 'bä'), startsWithCaseInsensitive('Bär', 'BÄ');

SELECT '-- Test startsWithCaseInsensitive UTF8 non-Latin';
SELECT startsWithCaseInsensitive('中国', '中'), startsWithCaseInsensitive('中国', '国');

SELECT '-- Test startsWithCaseInsensitiveUTF8 ASCII';
SELECT startsWithCaseInsensitiveUTF8('Marta', 'mA'), startsWithCaseInsensitiveUTF8('match_me_not', 'Na');

SELECT '-- Test startsWithCaseInsensitiveUTF8 UTF8 Latin';
SELECT startsWithCaseInsensitiveUTF8('Bär', 'bä'), startsWithCaseInsensitiveUTF8('Bär', 'BÄ');

SELECT '-- Test startsWithCaseInsensitiveUTF8 UTF8 non-Latin';
SELECT startsWithCaseInsensitiveUTF8('中国', '中'), startsWithCaseInsensitiveUTF8('Hello中国', '中');

SELECT '-- Test endsWithCaseInsensitive ASCII';
SELECT endsWithCaseInsensitive('Marta', 'tA'), endsWithCaseInsensitive('match_me_not', 'Na');

SELECT '-- Test endsWithCaseInsensitive UTF8 Latin';
SELECT endsWithCaseInsensitive('Bär', 'äR'), endsWithCaseInsensitive('Bär', 'ÄR');

SELECT '-- Test endsWithCaseInsensitive UTF8 non-Latin';
SELECT endsWithCaseInsensitive('中国', '国'), endsWithCaseInsensitive('中国', '中');

SELECT '-- Test endsWithCaseInsensitiveUTF8 ASCII';
SELECT endsWithCaseInsensitiveUTF8('Marta', 'tA'), endsWithCaseInsensitiveUTF8('match_me_not', 'Na');

SELECT '-- Test endsWithCaseInsensitiveUTF8 UTF8 Latin';
SELECT endsWithCaseInsensitiveUTF8('Bär', 'äR'), endsWithCaseInsensitiveUTF8('Bär', 'ÄR');

SELECT '-- Test endsWithCaseInsensitiveUTF8 UTF8 non-Latin';
SELECT endsWithCaseInsensitiveUTF8('中国', '国'), endsWithCaseInsensitiveUTF8('中国', '中');

SELECT '-- Test invalid UTF8';
SELECT startsWithCaseInsensitive('中国', '\xe4'), startsWithCaseInsensitiveUTF8('中国', '\xe4');
SELECT endsWithCaseInsensitive('中国', '\xbd'), endsWithCaseInsensitiveUTF8('中国', '\xbd');

DROP TABLE IF EXISTS tab;

CREATE TABLE tab(S1 String, S2 String, S3 FixedString(4)) ENGINE=Memory;
INSERT INTO tab values ('1a', 'a', 'AbA'), ('22', 'A', 'ab'), ('中国', '中', '国');

SELECT '-- Test constant needle with haystack - prefix';
SELECT COUNT() FROM tab WHERE startsWithCaseInsensitive(S1, '1');
SELECT COUNT() FROM tab WHERE startsWithCaseInsensitive(S2, '中');
SELECT COUNT() FROM tab WHERE startsWithCaseInsensitive(S3, '国');

SELECT COUNT() FROM tab WHERE startsWithCaseInsensitiveUTF8(S1, '1');
SELECT COUNT() FROM tab WHERE startsWithCaseInsensitiveUTF8(S2, '中');
-- startsWithCaseCaseInsensitiveUTF8 does not support FixedString

SELECT '-- Test constant needle with haystack - suffix';
SELECT COUNT() FROM tab WHERE endsWithCaseInsensitive(S1, '2');
SELECT COUNT() FROM tab WHERE endsWithCaseInsensitive(S2, '中');
SELECT COUNT() FROM tab WHERE endsWithCaseInsensitive(S3, '国\0');

SELECT COUNT() FROM tab WHERE endsWithCaseInsensitiveUTF8(S1, '2');
SELECT COUNT() FROM tab WHERE endsWithCaseInsensitiveUTF8(S2, '中');
-- endsWithCaseCaseInsensitiveUTF8 does not support FixedString

SELECT '-- Test column needle with haystack - prefix';
SELECT COUNT() FROM tab WHERE startsWithCaseInsensitive(S1, S1);
SELECT COUNT() FROM tab WHERE startsWithCaseInsensitive(S1, S2);
SELECT COUNT() FROM tab WHERE startsWithCaseInsensitive(S2, S3);

SELECT COUNT() FROM tab WHERE startsWithCaseInsensitiveUTF8(S1, S1);
SELECT COUNT() FROM tab WHERE startsWithCaseInsensitiveUTF8(S1, S2);
-- endsWithCaseCaseInsensitiveUTF8 does not support FixedString

SELECT '-- Test column needle with haystack - suffix';
SELECT COUNT() FROM tab WHERE endsWithCaseInsensitive(S1, S1);
SELECT COUNT() FROM tab WHERE endsWithCaseInsensitive(S1, S2);
SELECT COUNT() FROM tab WHERE endsWithCaseInsensitive(S2, S3);

SELECT COUNT() FROM tab WHERE endsWithCaseInsensitiveUTF8(S1, S1);
SELECT COUNT() FROM tab WHERE endsWithCaseInsensitiveUTF8(S1, S2);
-- endsWithCaseCaseInsensitiveUTF8 does not support FixedString

DROP TABLE tab;
