-- Tags: no-fasttest, use-vectorscan

-- The text-index tokenizers that back substring/regexp matching (e.g. `ngrams`) advance by UTF-8 code
-- points, so the index only ever stores code-point-aligned tokens. Some byte-oriented functions can match
-- a needle/pattern that is not valid UTF-8 (e.g. one that starts in the middle of a multi-byte character).
-- Such a needle byte-matches the row but tokenizes into grams that are never present in a code-point-aligned
-- index, so the index must NOT prune those granules -- it has to decline the optimization and fall back to a
-- full scan. This affects `multiSearchAny`, `multiSearchAnyUTF8` and `match` (RE2 falls back to Latin-1 for
-- non-UTF-8 patterns). `multiMatchAny` is different: Hyperscan rejects a non-UTF-8 pattern with an exception,
-- so the index must raise the same exception rather than prune. Every query below must therefore behave the
-- same with the index (`use_skip_indexes = 1`) as without it (`use_skip_indexes = 0`).
--
-- Note: correctness here assumes the indexed column itself contains valid UTF-8 -- the same assumption the
-- existing `like`/substring paths rely on. A non-UTF-8 value stored in the column is out of scope.

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    str String,
    INDEX idx str TYPE text(tokenizer = ngrams(3))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

-- Row 1 is the bytes C3 A9 62 63 = "ébc" (valid UTF-8: é, b, c). The needle used below, the bytes A9 62 63,
-- starts in the middle of the 'é' character: it is not valid UTF-8, but it is a byte-substring of row 1, so
-- the byte-oriented functions match it. The index stores only the code-point-aligned gram C3A96263.
INSERT INTO tab VALUES (1, unhex('C3A96263')), (2, 'hello world'), (3, 'foo bar baz');

SELECT '-- multiSearchAny with a non-UTF-8 needle must not prune the matching granule (expect 1 and 1)';
SELECT count() FROM tab WHERE multiSearchAny(str, [unhex('A96263')]) SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE multiSearchAny(str, [unhex('A96263')]) SETTINGS use_skip_indexes = 1;

SELECT '-- multiSearchAnyUTF8 with a non-UTF-8 needle must not prune the matching granule (expect 1 and 1)';
SELECT count() FROM tab WHERE multiSearchAnyUTF8(str, [unhex('A96263')]) SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE multiSearchAnyUTF8(str, [unhex('A96263')]) SETTINGS use_skip_indexes = 1;

SELECT '-- match with a non-UTF-8 pattern must not prune the matching granule (expect 1 and 1)';
SELECT count() FROM tab WHERE match(str, unhex('A96263')) SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE match(str, unhex('A96263')) SETTINGS use_skip_indexes = 1;

SELECT '-- multiMatchAny rejects a non-UTF-8 pattern; the index must raise the same exception, not prune';
SELECT count() FROM tab WHERE multiMatchAny(str, [unhex('A96263')]) SETTINGS use_skip_indexes = 0; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM tab WHERE multiMatchAny(str, [unhex('A96263')]) SETTINGS use_skip_indexes = 1; -- { serverError BAD_ARGUMENTS }

SELECT '-- one non-UTF-8 needle in the OR-set disables pruning for the whole multiSearchAny (expect 1 and 1)';
SELECT count() FROM tab WHERE multiSearchAny(str, ['nomatch', unhex('A96263')]) SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE multiSearchAny(str, ['nomatch', unhex('A96263')]) SETTINGS use_skip_indexes = 1;

SELECT '-- valid UTF-8 needles still use the index and return the matching rows';
SELECT id FROM tab WHERE multiSearchAny(str, ['wor']) ORDER BY id SETTINGS use_skip_indexes = 1;
SELECT id FROM tab WHERE match(str, 'bar') ORDER BY id SETTINGS use_skip_indexes = 1;
SELECT id FROM tab WHERE multiMatchAny(str, ['baz']) ORDER BY id SETTINGS use_skip_indexes = 1;

SELECT '-- a valid multi-byte UTF-8 needle is code-point aligned and matches through the index (expect 1 and 1)';
SELECT count() FROM tab WHERE multiSearchAny(str, [unhex('C3A96263')]) SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE multiSearchAny(str, [unhex('C3A96263')]) SETTINGS use_skip_indexes = 1;

DROP TABLE tab;

-- A preprocessor must not prune those granules either. The index takes its required tokens from
-- `preprocessor(needle)`, which bounds the tokens of `preprocessor(value)` only when the preprocessor maps
-- each character independently. The two cases below sit here rather than in the preprocessor test file
-- because `lowerUTF8` needs ICU and `multiMatchAny` needs Vectorscan, and that file runs in Fast test,
-- which is built with neither.

-- ICU case mapping is context sensitive: a Greek capital sigma lowercases to the final form only at the end
-- of a word, so the value 'ΣΟΣΑ' becomes 'σοσα' while the needle 'ΣΟΣ' becomes 'σος', whose gram 'ος' is
-- nowhere in the index.
CREATE TABLE tab
(
    str String,
    INDEX idx str TYPE text(tokenizer = ngrams(2), preprocessor = lowerUTF8(str))
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO tab VALUES ('ΣΟΣΑ');

SELECT '-- lowerUTF8 preprocessor: startsWith must not prune the matching granule (expect 1 and 1)';
SELECT count() FROM tab WHERE startsWith(str, 'ΣΟΣ') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE startsWith(str, 'ΣΟΣ') SETTINGS use_skip_indexes = 1;

SELECT '-- lowerUTF8 preprocessor: like must not prune the matching granule (expect 1 and 1)';
SELECT count() FROM tab WHERE str LIKE '%ΣΟΣ%' SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE str LIKE '%ΣΟΣ%' SETTINGS use_skip_indexes = 1;

DROP TABLE tab;

-- soundex folds a whole word into a four-character code, so no character of the needle survives in place:
-- 'hello, world!' becomes H464 while the needle 'hello' becomes H400.
CREATE TABLE tab
(
    str String,
    INDEX idx str TYPE text(tokenizer = ngrams(2), preprocessor = soundex(lower(str)))
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO tab VALUES ('Hello, world!');

SELECT '-- soundex preprocessor: multiMatchAny must not prune the matching granule (expect 1 and 1)';
SELECT count() FROM tab WHERE multiMatchAny(str, ['Hello']) SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE multiMatchAny(str, ['Hello']) SETTINGS use_skip_indexes = 1;

DROP TABLE tab;
