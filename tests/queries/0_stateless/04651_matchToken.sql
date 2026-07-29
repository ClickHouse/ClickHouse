-- Test matchToken function correctness (no text index; row-level evaluation).

DROP TABLE IF EXISTS test_match_token;

CREATE TABLE test_match_token
(
    id UInt32,
    message String
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO test_match_token VALUES
    (1, 'error occurred in the system'),
    (2, 'hello world from clickhouse'),
    (3, 'errno is set to invalid value'),
    (4, 'connection error handler'),
    (5, 'nothing matches here'),
    (6, ''),
    (7, 'ERRor case sensitive test'),
    (8, 'test123 numeric token'),
    (9, 'err_code is 42'),
    (10, 'warnings and errors everywhere'),
    (11, 'café résumé naïve');

-- ============================================================
-- Basic regexp patterns
-- ============================================================
SELECT '-- regexp: err.* (prefix-like)';
SELECT id, message FROM test_match_token WHERE matchToken(message, 'err.*') ORDER BY id;

SELECT '-- regexp: .*or (suffix-like)';
SELECT id, message FROM test_match_token WHERE matchToken(message, '.*or') ORDER BY id;

SELECT '-- regexp: e.ror (single char)';
SELECT id, message FROM test_match_token WHERE matchToken(message, 'e.ror') ORDER BY id;

SELECT '-- regexp: err(or|no) (alternation)';
SELECT id, message FROM test_match_token WHERE matchToken(message, 'err(or|no)') ORDER BY id;

SELECT '-- regexp: [a-z]rror (character class)';
SELECT id, message FROM test_match_token WHERE matchToken(message, '[a-z]rror') ORDER BY id;

SELECT '-- regexp: test[0-9]+ (numeric suffix)';
SELECT id, message FROM test_match_token WHERE matchToken(message, 'test[0-9]+') ORDER BY id;

SELECT '-- regexp: exact match error';
SELECT id, message FROM test_match_token WHERE matchToken(message, 'error') ORDER BY id;

SELECT '-- regexp: .* (match all)';
SELECT id, message FROM test_match_token WHERE matchToken(message, '.*') ORDER BY id;

SELECT '-- regexp: no match xyz.*';
SELECT id, message FROM test_match_token WHERE matchToken(message, 'xyz.*') ORDER BY id;

-- ============================================================
-- Edge cases
-- ============================================================
SELECT '-- edge: case sensitive (ERR.* matches ERRor, not lower-case error)';
SELECT id, message FROM test_match_token WHERE matchToken(message, 'ERR.*') ORDER BY id;

-- ============================================================
-- Constant folding tests
-- ============================================================
SELECT '-- literal tests';
SELECT matchToken('hello world', 'hel.*');
SELECT matchToken('hello world', '.*rld');
SELECT matchToken('hello world', 'h.llo');
SELECT matchToken('hello world', 'xyz.*');
SELECT matchToken('hello world', '.*');
SELECT matchToken('', '.*');

-- ============================================================
-- Combined with other predicates
-- ============================================================
SELECT '-- combined: matchToken AND hasAnyTokens';
SELECT id, message FROM test_match_token WHERE matchToken(message, 'err.*') AND hasAnyTokens(message, ['handler']) ORDER BY id;

-- ============================================================
-- Supplementary: anchored prefix ^err
-- An anchored regexp exposes a prefix to the text index (dictionary range-scan
-- candidate). Behavior must equal the unanchored equivalent here.
-- ============================================================
SELECT '-- supp: anchored ^err matches tokens starting with err';
SELECT id, message FROM test_match_token WHERE matchToken(message, '^err') ORDER BY id;

-- ============================================================
-- Supplementary: unanchored pattern without leading .*
-- A bare substring pattern (no ^, no leading .*) has no anchored prefix and
-- must still match any token containing the substring.
-- ============================================================
SELECT '-- supp: bare substring ror matches token containing ror';
SELECT id, message FROM test_match_token WHERE matchToken(message, 'ror') ORDER BY id;

-- ============================================================
-- Supplementary: top-level alternation with no shared anchored prefix
-- ============================================================
SELECT '-- supp: top-level alternation hello|nothing';
SELECT id, message FROM test_match_token WHERE matchToken(message, 'hello|nothing') ORDER BY id;

-- ============================================================
-- Supplementary: empty pattern
-- An empty regexp matches every token (the empty string is a substring of any
-- token), so it matches every row that has at least one token.
-- ============================================================
SELECT '-- supp: empty pattern matches rows with at least one token';
SELECT id, message FROM test_match_token WHERE matchToken(message, '') ORDER BY id;

-- ============================================================
-- Supplementary: . does not cross a token separator
-- '.' matches one character within a token; it must not bridge tokens split by
-- a non-alphanumeric separator (err.code should not match err_code).
-- ============================================================
SELECT '-- supp: . does not cross separator (err.code should not match err_code)';
SELECT id, message FROM test_match_token WHERE matchToken(message, 'err.code') ORDER BY id;

-- ============================================================
-- Supplementary: non-ASCII token regexp
-- splitByNonAlpha treats a run of non-ASCII bytes as a single token; a regexp
-- with an anchored non-ASCII prefix matches that token.
-- ============================================================
SELECT '-- supp: non-ASCII token regexp prefix';
SELECT id, message FROM test_match_token WHERE matchToken(message, 'caf.*') ORDER BY id;

-- ============================================================
-- Array(String) haystack tests (ported from 04413_hasTokenRegexp_array)
-- ============================================================
SELECT '-- array literal: regexp prefix-like matches across elements';
SELECT matchToken(['error occurred', 'misc tag'], 'err.*');

SELECT '-- array literal: regexp suffix-like matches across elements';
SELECT matchToken(['hello world', 'error handler'], '.*or');

SELECT '-- array literal: array tokenizer matches whole element regexp';
SELECT matchToken(['abc::def', 'ghi'], 'abc.*', 'array');

SELECT '-- array literal: split tokenizer can match inner token regexp';
SELECT matchToken(['abc::def', 'ghi'], 'd.f', 'splitByString([\'::\'])');

SELECT '-- array literal: nullable elements are ignored';
SELECT matchToken(CAST(['error occurred', NULL, 'misc tag'], 'Array(Nullable(String))'), 'err.*');

SELECT '-- array literal: empty array has no matching element';
SELECT matchToken(CAST([], 'Array(String)'), 'err.*');

SELECT '-- array literal: empty-string element contributes no token, other element matches';
SELECT matchToken(['', 'error handler'], 'err.*');

SELECT '-- array literal: all elements non-matching';
SELECT matchToken(['alpha', 'beta'], 'zzz.*');

-- ============================================================
-- Array column correctness (no text index; row-level evaluation)
-- ============================================================
DROP TABLE IF EXISTS test_match_token_array;

CREATE TABLE test_match_token_array
(
    id UInt32,
    tags Array(String)
)
ENGINE = MergeTree()
ORDER BY id;

INSERT INTO test_match_token_array VALUES
    (1, ['hello world', 'error handler']),
    (2, ['alpha beta', 'gamma delta']),
    (3, ['errno state', 'misc value']);

SELECT '-- array table: regexp err.* with asciiCJK tokenizer';
SELECT id, tags FROM test_match_token_array WHERE matchToken(tags, 'err.*', 'asciiCJK') ORDER BY id;

SELECT '-- array table: regexp err.* with splitByNonAlpha tokenizer';
SELECT id, tags FROM test_match_token_array WHERE matchToken(tags, 'err.*', 'splitByNonAlpha') ORDER BY id;

DROP TABLE test_match_token_array;

DROP TABLE test_match_token;
