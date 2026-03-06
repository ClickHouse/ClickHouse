-- Negative tests: parameter validation
SELECT caseFoldUTF8(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT caseFoldUTF8('x', 'aggressive', 1, 'extra'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT accentFoldUTF8('x', 1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT foldUTF8('x', 'aggressive', 1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT caseFoldUTF8(123); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT caseFoldUTF8('x', 123); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT caseFoldUTF8('x', 'aggressive', 'true'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT caseFoldUTF8('x', 'invalid'); -- { serverError BAD_ARGUMENTS }
SELECT caseFoldUTF8('x', 'aggressive', 1); -- { serverError BAD_ARGUMENTS }
SELECT foldUTF8('x', 'invalid'); -- { serverError BAD_ARGUMENTS }
SELECT caseFoldUTF8(toFixedString('hello', 5)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT accentFoldUTF8(toFixedString('hello', 5)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT foldUTF8(toFixedString('hello', 5)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- caseFoldUTF8: basic case folding
SELECT '-- caseFoldUTF8 aggressive (default)';
SELECT caseFoldUTF8('Hello World');
SELECT caseFoldUTF8('Straße');
SELECT caseFoldUTF8('HÉLLO');
SELECT caseFoldUTF8('ﬃ'); -- ffi ligature: aggressive NFKC decomposes it

SELECT '-- caseFoldUTF8 conservative';
SELECT caseFoldUTF8('Hello World', 'conservative');
SELECT caseFoldUTF8('Straße', 'conservative');
SELECT caseFoldUTF8('HÉLLO', 'conservative');
SELECT caseFoldUTF8('ﬃ', 'conservative'); -- conservative keeps ligature intact

-- accentFoldUTF8: diacritic removal
SELECT '-- accentFoldUTF8';
SELECT accentFoldUTF8('café résumé naïve');
SELECT accentFoldUTF8('Ångström');
SELECT accentFoldUTF8('piñata');

-- foldUTF8: combined case + accent folding
SELECT '-- foldUTF8 aggressive (default)';
SELECT foldUTF8('Café Résumé');
SELECT foldUTF8('HÉLLO Wörld');
SELECT foldUTF8('Straße');

SELECT '-- foldUTF8 conservative';
SELECT foldUTF8('Café Résumé', 'conservative');
SELECT foldUTF8('HÉLLO Wörld', 'conservative');

-- caseFoldUTF8 with exclude_special_I
SELECT '-- caseFoldUTF8 with special I handling';
SELECT caseFoldUTF8('İstanbul', 'conservative', 0);
SELECT caseFoldUTF8('İstanbul', 'conservative', 1);

-- Empty string
SELECT '-- empty strings';
SELECT caseFoldUTF8('');
SELECT accentFoldUTF8('');
SELECT foldUTF8('');

-- ASCII-only (no-op for accent fold)
SELECT '-- ASCII only';
SELECT caseFoldUTF8('ABC');
SELECT accentFoldUTF8('abc');
SELECT foldUTF8('ABC');

-- Multi-row table test
SELECT '-- table test';
DROP TABLE IF EXISTS test_fold_utf8;
CREATE TABLE test_fold_utf8 (s String) ENGINE = Memory;
INSERT INTO test_fold_utf8 VALUES ('Hello World'), ('Straße'), ('HÉLLO'), ('café résumé'), ('ﬃ'), ('');
SELECT s, caseFoldUTF8(s), accentFoldUTF8(s), foldUTF8(s) FROM test_fold_utf8 ORDER BY s;
DROP TABLE test_fold_utf8;
