-- Tags: no-fasttest
-- no-fasttest: the `normalize`, base64, and `word_stem` functions need the ICU, simdutf, and libstemmer libraries

SET allow_experimental_trino_dialect = 1;
SET dialect = 'trino';

SELECT '-- base64';
SELECT to_base64(from_base64('YWI='));
SELECT to_base64(CAST('hello world' AS VARBINARY));

SELECT '-- normalize';
SELECT normalize('schön', NFD);
SELECT normalize('schön');
SELECT normalize('schön', NFC);
SELECT normalize('schön', NFKD);
SELECT normalize('schön', NFKC);
SELECT normalize('㈱㌧㌦Ⅲ', NFKC);
SELECT normalize('ﾊﾝｶｸｶﾅ', NFKC);

SELECT '-- word_stem';
SELECT word_stem('');
SELECT word_stem('x');
SELECT word_stem('abc');
SELECT word_stem('generally');
SELECT word_stem('useful');
SELECT word_stem('runs');
SELECT word_stem('run');
SELECT word_stem('authorized', 'en');
SELECT word_stem('accessories', 'en');
SELECT word_stem('intensifying', 'en');
SELECT word_stem('resentment');
SELECT word_stem('faithfulness');
SELECT word_stem('continuerait', 'fr');
SELECT word_stem('torpedearon', 'es');
SELECT word_stem('quilomtricos', 'pt');
SELECT word_stem('pronunziare', 'it');
SELECT word_stem('auferstnde', 'de');
