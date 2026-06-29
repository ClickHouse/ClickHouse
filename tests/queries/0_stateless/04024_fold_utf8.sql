-- Tags: no-fasttest
-- no-fasttest: requires ICU library

-- Negative tests: parameter validation
SELECT caseFoldUTF8(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT caseFoldUTF8('x', 'extra'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT removeDiacriticsUTF8('x', 1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT caseFoldUTF8(123); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT caseFoldUTF8(toFixedString('hello', 5)); -- { serverError ILLEGAL_COLUMN }
SELECT removeDiacriticsUTF8(toFixedString('hello', 5)); -- { serverError ILLEGAL_COLUMN }

-- caseFoldUTF8: basic case folding
SELECT '-- caseFoldUTF8';
SELECT caseFoldUTF8('Hello World');
SELECT caseFoldUTF8('Straße');
SELECT caseFoldUTF8('HÉLLO');
SELECT caseFoldUTF8('ﬃ'); -- case folding decomposes ffi ligature
SELECT caseFoldUTF8('Ⅷ'); -- Roman numeral: preserved (no compatibility decomposition)

-- removeDiacriticsUTF8: diacritic removal
SELECT '-- removeDiacriticsUTF8';
SELECT removeDiacriticsUTF8('café résumé naïve');
SELECT removeDiacriticsUTF8('Ångström');
SELECT removeDiacriticsUTF8('piñata');

-- Empty string
SELECT '-- empty strings';
SELECT caseFoldUTF8('');
SELECT removeDiacriticsUTF8('');

-- Single character inputs
SELECT '-- single chars';
SELECT caseFoldUTF8('A'), caseFoldUTF8('a'), caseFoldUTF8('é'), caseFoldUTF8('Ω');
SELECT removeDiacriticsUTF8('A'), removeDiacriticsUTF8('a'), removeDiacriticsUTF8('é'), removeDiacriticsUTF8('Ω');

-- ASCII-only (no-op for accent fold)
SELECT '-- ASCII only';
SELECT caseFoldUTF8('ABC');
SELECT caseFoldUTF8('abc');
SELECT removeDiacriticsUTF8('abc');

-- Supplementary plane characters (surrogate pairs in UTF-16)
SELECT '-- supplementary plane';
SELECT caseFoldUTF8('𝐀𝐁𝐂');
SELECT removeDiacriticsUTF8('𝐀𝐁𝐂');
SELECT caseFoldUTF8('Hello 🌍');

-- String of only combining marks (should produce empty string)
SELECT '-- only combining marks';
SELECT removeDiacriticsUTF8(char(0xCC, 0x81, 0xCC, 0x88));

-- Multiple accents on one base character
SELECT '-- multiple accents';
SELECT removeDiacriticsUTF8('ạ̈');

-- CJK passthrough (no case, no accents)
SELECT '-- CJK passthrough';
SELECT caseFoldUTF8('日本語テスト'), removeDiacriticsUTF8('日本語テスト');

-- removeAccentsUTF8 alias
SELECT '-- removeAccentsUTF8 alias';
SELECT removeAccentsUTF8('café résumé naïve');
SELECT removeAccentsUTF8('Ångström');

-- Idempotency: applying twice gives the same result
SELECT '-- idempotency';
SELECT caseFoldUTF8(caseFoldUTF8('Straße')) = caseFoldUTF8('Straße');
SELECT removeDiacriticsUTF8(removeDiacriticsUTF8('piñata')) = removeDiacriticsUTF8('piñata');

-- Multi-row table test
SELECT '-- table test';
DROP TABLE IF EXISTS test_fold_utf8;
CREATE TABLE test_fold_utf8 (s String) ENGINE = Memory;
INSERT INTO test_fold_utf8 VALUES ('Hello World'), ('Straße'), ('HÉLLO'), ('café résumé'), ('ﬃ'), ('');
SELECT s, caseFoldUTF8(s), removeDiacriticsUTF8(s) FROM test_fold_utf8 ORDER BY s;
DROP TABLE test_fold_utf8;

-- Text index preprocessor tests
SELECT '-- text index: caseFoldUTF8 preprocessor';
DROP TABLE IF EXISTS test_fold_text_index;
CREATE TABLE test_fold_text_index
(
    id UInt64,
    val String,
    INDEX idx_case(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = caseFoldUTF8(val))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test_fold_text_index VALUES (1, 'Hello World'), (2, 'café résumé'), (3, 'Straße');

SELECT count() FROM test_fold_text_index WHERE hasToken(val, 'hello');
SELECT count() FROM test_fold_text_index WHERE hasToken(val, 'HELLO');
SELECT count() FROM test_fold_text_index WHERE hasToken(val, 'Hello');
SELECT count() FROM test_fold_text_index WHERE hasToken(val, 'strasse');
SELECT count() FROM test_fold_text_index WHERE hasToken(val, 'STRASSE');
SELECT count() FROM test_fold_text_index WHERE hasToken(val, 'missing');

DROP TABLE test_fold_text_index;

SELECT '-- text index: removeDiacriticsUTF8 preprocessor';
CREATE TABLE test_fold_text_index
(
    id UInt64,
    val String,
    INDEX idx_accent(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = removeDiacriticsUTF8(val))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test_fold_text_index VALUES (1, 'café résumé'), (2, 'piñata fiesta'), (3, 'hello world');

SELECT count() FROM test_fold_text_index WHERE hasToken(val, 'cafe');
SELECT count() FROM test_fold_text_index WHERE hasToken(val, 'café'); -- accent is part of the e
SELECT count() FROM test_fold_text_index WHERE hasToken(val, 'café'); -- accent is after the e
SELECT count() FROM test_fold_text_index WHERE hasToken(val, 'resume');
SELECT count() FROM test_fold_text_index WHERE hasToken(val, 'pinata');
SELECT count() FROM test_fold_text_index WHERE hasToken(val, 'hello');
SELECT count() FROM test_fold_text_index WHERE hasToken(val, 'missing');

DROP TABLE test_fold_text_index;

SELECT '-- text index: combined removeDiacriticsUTF8(caseFoldUTF8) preprocessor';
CREATE TABLE test_fold_text_index
(
    id UInt64,
    val String,
    INDEX idx_both(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = removeDiacriticsUTF8(caseFoldUTF8(val)))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test_fold_text_index VALUES (1, 'CAFÉ Résumé'), (2, 'Piñata FIESTA'), (3, 'Hello World');

SELECT count() FROM test_fold_text_index WHERE hasToken(val, 'cafe');
SELECT count() FROM test_fold_text_index WHERE hasToken(val, 'café'); -- accent is part of the e
SELECT count() FROM test_fold_text_index WHERE hasToken(val, 'café'); -- accent is after the e
SELECT count() FROM test_fold_text_index WHERE hasToken(val, 'CAFE');
SELECT count() FROM test_fold_text_index WHERE hasToken(val, 'resume');
SELECT count() FROM test_fold_text_index WHERE hasToken(val, 'pinata');
SELECT count() FROM test_fold_text_index WHERE hasToken(val, 'PINATA');
SELECT count() FROM test_fold_text_index WHERE hasToken(val, 'fiesta');
SELECT count() FROM test_fold_text_index WHERE hasToken(val, 'FIESTA');
SELECT count() FROM test_fold_text_index WHERE hasToken(val, 'hello');
SELECT count() FROM test_fold_text_index WHERE hasToken(val, 'missing');

DROP TABLE test_fold_text_index;
