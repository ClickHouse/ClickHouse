-- Tags: no-parallel-replicas

-- Tests the `match_tokens` argument of the `splitByRegexp` tokenizer for text indexes.
-- `force_data_skipping_indices` ensures every search below is actually served by the index.

-- 1. Capture group 1 becomes the token; the 'tag:' prefix is not indexed.

DROP TABLE IF EXISTS tab_regex_extract;

CREATE TABLE tab_regex_extract
(
    id UInt64,
    doc String,
    INDEX idx doc TYPE text(tokenizer = splitByRegexp('tag:(\\w+)', 1))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2;

INSERT INTO tab_regex_extract VALUES (1, 'tag:red tag:green'), (2, 'tag:blue'), (3, 'no tags here'), (4, 'tag:green tag:yellow');

SELECT 'match_tokens: tokens of each row';
SELECT id, tokens(doc, $$splitByRegexp('tag:(\\w+)', 1)$$) FROM tab_regex_extract ORDER BY id;

SELECT 'match_tokens: hasAnyTokens([green]) -> 1, 4';
SELECT id FROM tab_regex_extract WHERE hasAnyTokens(doc, ['green']) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';

SELECT 'match_tokens: hasAnyTokens([tag]) -> (none, the prefix is not indexed)';
SELECT id FROM tab_regex_extract WHERE hasAnyTokens(doc, ['tag']) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';

SELECT 'match_tokens: hasAllTokens([red, green]) -> 1';
SELECT id FROM tab_regex_extract WHERE hasAllTokens(doc, ['red', 'green']) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE tab_regex_extract;

-- 2. No capture groups: falls back to indexing the RE2 matches themselves,
-- unlike the default separator mode which would index the text *between* the digit runs.

DROP TABLE IF EXISTS tab_regex_extract_nogroup;

CREATE TABLE tab_regex_extract_nogroup
(
    id UInt64,
    doc String,
    INDEX idx doc TYPE text(tokenizer = splitByRegexp('[0-9]+', 1))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2;

INSERT INTO tab_regex_extract_nogroup VALUES (1, 'a1b22c333'), (2, 'x99y'), (3, 'no digits here');

SELECT 'nogroup: tokens of each row';
SELECT id, tokens(doc, $$splitByRegexp('[0-9]+', 1)$$) FROM tab_regex_extract_nogroup ORDER BY id;

SELECT 'nogroup: hasAnyTokens([22]) -> 1';
SELECT id FROM tab_regex_extract_nogroup WHERE hasAnyTokens(doc, ['22']) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';

SELECT 'nogroup: hasAnyTokens([a]) -> (none, letters are never matches)';
SELECT id FROM tab_regex_extract_nogroup WHERE hasAnyTokens(doc, ['a']) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE tab_regex_extract_nogroup;

-- 3. `hasPhrase` on a `splitByRegexp` index combined with a postprocessor stays rejected regardless of
-- `match_tokens` - the row-level rewrite still assumes whitespace-splitting, `splitByNonAlpha`-style tokens.

DROP TABLE IF EXISTS tab_extract_phrase_pp;

CREATE TABLE tab_extract_phrase_pp
(
    id UInt64,
    doc String,
    INDEX idx doc TYPE text(tokenizer = splitByRegexp('tag:(\\w+)', 1), postprocessor = lower(doc))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2;

INSERT INTO tab_extract_phrase_pp VALUES (1, 'tag:Red tag:Green');

SELECT id FROM tab_extract_phrase_pp WHERE hasPhrase(doc, 'red green') SETTINGS use_skip_indexes = 1; -- { serverError BAD_ARGUMENTS }

DROP TABLE tab_extract_phrase_pp;

-- 4. DDL-time validation of `match_tokens`, and the `true`/`false` literal form (section 1 already covers 1).

SELECT 'DDL validation';

DROP TABLE IF EXISTS tab_bad_extract;

-- match_tokens must be a Bool or an integer (truthy/falsy); a non-numeric type is rejected
CREATE TABLE tab_bad_extract (id UInt64, doc String, INDEX idx doc TYPE text(tokenizer = splitByRegexp('a', 'x'))) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }
-- splitByRegexp accepts at most 2 parameters
CREATE TABLE tab_bad_extract (id UInt64, doc String, INDEX idx doc TYPE text(tokenizer = splitByRegexp('a', 1, 1))) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }
-- A pattern that can match an empty string is rejected too, same as through the tokens() function
CREATE TABLE tab_bad_extract (id UInt64, doc String, INDEX idx doc TYPE text(tokenizer = splitByRegexp('[0-9]*', true))) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS tab_extract_bool_literal;

CREATE TABLE tab_extract_bool_literal
(
    id UInt64,
    doc String,
    INDEX idx doc TYPE text(tokenizer = splitByRegexp('tag:(\\w+)', true))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2;

INSERT INTO tab_extract_bool_literal VALUES (1, 'tag:red tag:green'), (2, 'tag:blue');

SELECT 'bool literal: hasAnyTokens([green]) -> 1';
SELECT id FROM tab_extract_bool_literal WHERE hasAnyTokens(doc, ['green']) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE tab_extract_bool_literal;
