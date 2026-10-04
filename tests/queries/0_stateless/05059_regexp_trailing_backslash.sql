-- Regression for #117853.
-- A trailing unescaped backslash is an invalid RE2 regexp and must not
-- be treated as a trivial substring.

-- These patterns were incorrectly handled as trivial substring searches.
SELECT match('abcd', 'abc\\'); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT match('abcd', '\\'); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT extractAll('abcd', 'abc\\'); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT countMatches('abcabc', 'abc\\'); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT splitByRegexp('abc\\', 'xabcy'); -- { serverError CANNOT_COMPILE_REGEXP }

-- Non-trivial forms of the same invalid regexp were already rejected.
SELECT match('abcd', 'ab(c)\\'); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT match('abcd', '^abc\\'); -- { serverError CANNOT_COMPILE_REGEXP }

-- Other regexp consumers already reject the same invalid regexp.
-- Keep their existing error-code behaviour.
SELECT replaceRegexpAll('abcd', 'abc\\', 'X'); -- { serverError BAD_ARGUMENTS }

-- Valid escaped backslashes must continue working.
SELECT match('abc\\', 'abc\\\\');
SELECT extractAll('abc\\', 'abc\\\\');
SELECT countMatches('abc\\abc\\', 'abc\\\\');
SELECT splitByRegexp('abc\\\\', 'xabc\\y');

-- Invalid regexp must not be hidden by an ngram bloom-filter skip index.
DROP TABLE IF EXISTS regexp_ngram_idx;

CREATE TABLE regexp_ngram_idx
(
    s String,
    INDEX idx s TYPE ngrambf_v1(3, 512, 2, 0)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO regexp_ngram_idx VALUES ('nothing here');

SELECT count()
FROM regexp_ngram_idx
WHERE match(s, 'abc\\')
SETTINGS force_data_skipping_indices = 'idx'; -- { serverError CANNOT_COMPILE_REGEXP }

DROP TABLE regexp_ngram_idx;

-- tokenbf_v1 uses the same regexp skip-index condition.
DROP TABLE IF EXISTS regexp_token_idx;

CREATE TABLE regexp_token_idx
(
    s String,
    INDEX idx s TYPE tokenbf_v1(512, 2, 0)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO regexp_token_idx VALUES ('nothing here');

SELECT count()
FROM regexp_token_idx
WHERE match(s, 'abc\\')
SETTINGS force_data_skipping_indices = 'idx'; -- { serverError CANNOT_COMPILE_REGEXP }

DROP TABLE regexp_token_idx;

-- A full-text index with an invalid splitByRegexp tokenizer regexp
-- must fail when the index is created.
DROP TABLE IF EXISTS regexp_text_invalid_tokenizer;

CREATE TABLE regexp_text_invalid_tokenizer
(
    s String,
    INDEX idx s TYPE text(tokenizer = splitByRegexp('abc\\'))
)
ENGINE = MergeTree
ORDER BY tuple(); -- { serverError CANNOT_COMPILE_REGEXP }

-- A full-text index with the default splitByNonAlpha tokenizer must
-- still reject an invalid regexp used by match().
DROP TABLE IF EXISTS regexp_text_idx;

CREATE TABLE regexp_text_idx
(
    s String,
    INDEX idx s TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO regexp_text_idx VALUES ('nothing here');

SELECT count()
FROM regexp_text_idx
WHERE match(s, 'abc\\')
SETTINGS force_data_skipping_indices = 'idx'; -- { serverError CANNOT_COMPILE_REGEXP }

DROP TABLE regexp_text_idx;