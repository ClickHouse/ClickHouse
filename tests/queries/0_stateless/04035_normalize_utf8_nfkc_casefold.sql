-- Tags: no-fasttest

-- Empty string
SELECT '' AS value, normalizeUTF8NFKCCasefold(value) AS result, length(result);

-- ASCII: case folding
SELECT 'Hello World' AS value, normalizeUTF8NFKCCasefold(value) AS result;
SELECT 'ABCDEFGHIJKLMNOPQRSTUVWXYZ' AS value, normalizeUTF8NFKCCasefold(value) AS result;
SELECT 'already lowercase' AS value, normalizeUTF8NFKCCasefold(value) AS result;

-- Numbers and punctuation (unchanged)
SELECT '0123456789' AS value, normalizeUTF8NFKCCasefold(value) AS result;
SELECT '!@#$%^&*()' AS value, normalizeUTF8NFKCCasefold(value) AS result;

-- German sharp s: ß case-folds to ss
SELECT 'Straße' AS value, normalizeUTF8NFKCCasefold(value) AS result;

-- Greek case folding
SELECT 'ΩΔΣΦ' AS value, normalizeUTF8NFKCCasefold(value) AS result;
-- Greek final sigma: Σ at end of word case-folds to σ (not ς)
SELECT 'ΣΊΓΜΑ' AS value, normalizeUTF8NFKCCasefold(value) AS result;

-- Turkish dotted I (standard Unicode casefold, not locale-specific)
SELECT 'İ' AS value, normalizeUTF8NFKCCasefold(value) AS result, hex(result);

-- Compatibility decomposition + case folding: circled numbers
SELECT '① ② ③' AS value, normalizeUTF8NFKCCasefold(value) AS result;

-- Compatibility decomposition: ligatures
SELECT 'ﬁ' AS value, normalizeUTF8NFKCCasefold(value) AS result;
SELECT 'ﬃ' AS value, normalizeUTF8NFKCCasefold(value) AS result;

-- Full-width characters → ASCII + case folding
SELECT 'Ａ' AS value, normalizeUTF8NFKCCasefold(value) AS result;
SELECT 'ＡＢＣＤ' AS value, normalizeUTF8NFKCCasefold(value) AS result;

-- Composed vs decomposed: both should produce the same output
SELECT normalizeUTF8NFKCCasefold('É') = normalizeUTF8NFKCCasefold(concat('E', char(0xCC, 0x81)));

-- NFC composition after NFKC: e + combining acute → é (case-folded from É)
SELECT 'É' AS value, normalizeUTF8NFKCCasefold(value) AS result;

-- Arabic ligature (compatibility decomposition)
SELECT 'ﷺ' AS value, normalizeUTF8NFKCCasefold(value) AS result;

-- CJK characters (unchanged by case folding)
SELECT '本気ですか' AS value, normalizeUTF8NFKCCasefold(value) AS result;

-- Emoji (unchanged)
SELECT '🎉' AS value, normalizeUTF8NFKCCasefold(value) AS result;

-- Mixed content
SELECT 'Héllo Wörld 123 ①' AS value, normalizeUTF8NFKCCasefold(value) AS result;

-- Default ignorable code points (soft hyphen U+00AD is removed by NFKC_Casefold)
SELECT hex(normalizeUTF8NFKCCasefold(concat('ab', char(0xC2, 0xAD), 'cd')));

-- Idempotency: applying twice gives the same result
SELECT normalizeUTF8NFKCCasefold('Hello ① Straße') = normalizeUTF8NFKCCasefold(normalizeUTF8NFKCCasefold('Hello ① Straße'));

-- Case-insensitive matching use case: both should normalize to the same string
SELECT normalizeUTF8NFKCCasefold('HELLO') = normalizeUTF8NFKCCasefold('hello');
SELECT normalizeUTF8NFKCCasefold('Straße') = normalizeUTF8NFKCCasefold('STRASSE');

-- Comparison with normalizeUTF8NFKC: NFKC_Casefold additionally lowercases
SELECT normalizeUTF8NFKC('Hello') AS nfkc, normalizeUTF8NFKCCasefold('Hello') AS nfkc_cf;

-- Invalid UTF-8 should throw
SELECT char(228) AS value, normalizeUTF8NFKCCasefold(value); -- { serverError CANNOT_NORMALIZE_STRING }

-- Table usage
DROP TABLE IF EXISTS nfkc_cf_test;
CREATE TABLE nfkc_cf_test (id UInt32, value String) ENGINE = MergeTree ORDER BY id;
INSERT INTO nfkc_cf_test VALUES (1, 'Hello'), (2, 'HELLO'), (3, 'héllo'), (4, 'HÉLLO');
SELECT id, value, normalizeUTF8NFKCCasefold(value) AS normalized FROM nfkc_cf_test ORDER BY id;
DROP TABLE nfkc_cf_test;
