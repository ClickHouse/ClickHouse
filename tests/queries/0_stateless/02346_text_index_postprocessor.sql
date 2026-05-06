-- Tests the postprocessor argument in text indexes.
-- The postprocessor transforms each token individually after tokenization.

DROP TABLE IF EXISTS tab;

SELECT '1. Basic lower postprocessor.';

CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = lower(val))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, 'foo'), (2, 'BAR'), (3, 'Baz');

-- hasToken: postprocessor lowercases the search token, enabling case-insensitive search.
SELECT count() FROM tab WHERE hasToken(val, 'foo');
SELECT count() FROM tab WHERE hasToken(val, 'FOO');
SELECT count() FROM tab WHERE hasToken(val, 'BAR');
SELECT count() FROM tab WHERE hasToken(val, 'Baz');
SELECT count() FROM tab WHERE hasToken(val, 'bar');
SELECT count() FROM tab WHERE hasToken(val, 'baz');
SELECT count() FROM tab WHERE hasToken(val, 'def');

-- hasAllTokens with String needle.
SELECT count() FROM tab WHERE hasAllTokens(val, 'foo');
SELECT count() FROM tab WHERE hasAllTokens(val, 'FOO');
SELECT count() FROM tab WHERE hasAllTokens(val, 'BAR');
SELECT count() FROM tab WHERE hasAllTokens(val, 'Baz');

-- hasAllTokens with Array needle (no preprocessor on elements, but postprocessor applied).
SELECT count() FROM tab WHERE hasAllTokens(val, ['FOO']);
SELECT count() FROM tab WHERE hasAllTokens(val, ['FOO', 'BAR']);

-- hasAnyTokens with String needle.
SELECT count() FROM tab WHERE hasAnyTokens(val, 'foo');
SELECT count() FROM tab WHERE hasAnyTokens(val, 'FOO');
SELECT count() FROM tab WHERE hasAnyTokens(val, 'BAR');
SELECT count() FROM tab WHERE hasAnyTokens(val, 'Baz');

-- hasAnyTokens with Array needle.
SELECT count() FROM tab WHERE hasAnyTokens(val, ['FOO', 'BAR']);
SELECT count() FROM tab WHERE hasAnyTokens(val, ['def', 'xyz']);

DROP TABLE tab;

SELECT '2. Postprocessor using concat.';

-- Each token 't' is stored as concat(t, 's').
-- The same postprocessor applies to the search needle, so searching 'cat' → 'cats' (found),
-- but searching 'cats' → 'catss' (not found).
CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = concat(val, 's'))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, 'cat'), (2, 'dog');

SELECT count() FROM tab WHERE hasToken(val, 'cat');
SELECT count() FROM tab WHERE hasToken(val, 'cats');
SELECT count() FROM tab WHERE hasToken(val, 'dog');
SELECT count() FROM tab WHERE hasToken(val, 'dogs');

DROP TABLE tab;

SELECT '3. Postprocessor using substring (prefix matching).';

-- The postprocessor keeps only the first 3 characters.
-- Both 'hello' and 'help' map to prefix 'hel', so they occupy the same posting list.
CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = substring(val, 1, 3))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, 'hello'), (2, 'help'), (3, 'world');

-- Both row 1 and row 2 are indexed under 'hel', so both match queries for 'hello' or 'help'.
SELECT count() FROM tab WHERE hasToken(val, 'hello');
SELECT count() FROM tab WHERE hasToken(val, 'help');
SELECT count() FROM tab WHERE hasToken(val, 'world');
SELECT count() FROM tab WHERE hasToken(val, 'xyz');

DROP TABLE tab;

SELECT '4. Stop-word filtering postprocessor.';

-- Tokens that map to empty string are excluded from the index.
-- Searching a needle that contains a stop word correctly ignores it.
CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = if(val = 'the', '', val))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, 'hello world'), (2, 'foo bar');

-- 'hello' and 'world' are stored normally; 'foo' and 'bar' too.
SELECT count() FROM tab WHERE hasToken(val, 'hello');
SELECT count() FROM tab WHERE hasToken(val, 'world');
-- When 'the' appears in the needle string, it is filtered from the search tokens.
-- hasAllTokens(val, 'hello the world') is equivalent to hasAllTokens(val, 'hello world').
SELECT count() FROM tab WHERE hasAllTokens(val, 'hello world');
SELECT count() FROM tab WHERE hasAllTokens(val, 'hello the world');

DROP TABLE tab;

SELECT '5. Regex-based postprocessor.';

-- Strips the suffix 'ing' from each token (simple unstemming).
CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = replaceRegexpAll(val, 'ing$', ''))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, 'running'), (2, 'walking'), (3, 'cat');

-- Searching 'running' → strips 'ing' → 'runn' → found.
-- Searching 'run' → strips nothing → 'run' → not 'runn' → not found.
SELECT count() FROM tab WHERE hasToken(val, 'running');
SELECT count() FROM tab WHERE hasToken(val, 'run');
SELECT count() FROM tab WHERE hasToken(val, 'walking');
SELECT count() FROM tab WHERE hasToken(val, 'cat');

DROP TABLE tab;

SELECT '6. Combined preprocessor and postprocessor.';

-- The preprocessor lowercases the full string before tokenization.
-- The postprocessor strips vowels from each token.
CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(
        tokenizer = 'splitByNonAlpha',
        preprocessor = lower(val),
        postprocessor = replaceRegexpAll(val, '[aeiou]', '')
    )
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, 'Hello World'), (2, 'FOO BAR');

-- 'Hello World' → lower → 'hello world' → tokens: 'hello', 'world' → strip vowels → 'hll', 'wrld'
-- 'FOO BAR' → lower → 'foo bar' → tokens: 'foo', 'bar' → strip vowels → 'f', 'br'
SELECT count() FROM tab WHERE hasToken(val, 'Hello');
SELECT count() FROM tab WHERE hasToken(val, 'WORLD');
SELECT count() FROM tab WHERE hasToken(val, 'FOO');
SELECT count() FROM tab WHERE hasToken(val, 'BAR');
SELECT count() FROM tab WHERE hasToken(val, 'xyz');

DROP TABLE tab;

SELECT '7. Partially materialized index.';

-- The index is added after the initial insert, so old parts have no index.
-- The postprocessor is applied to the needle at the query plan level in both cases:
-- for new parts the index is used; for old parts the postprocessed needle is used in a row scan.
DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id UInt64, val String) ENGINE = MergeTree ORDER BY id;

SYSTEM STOP MERGES tab;

INSERT INTO tab VALUES (1, 'foo'), (2, 'bar');

ALTER TABLE tab ADD INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = lower(val));

INSERT INTO tab VALUES (3, 'baz'), (4, 'QUX');

-- Old parts (no index): row-level scan uses the postprocessed (lowercased) needle.
SELECT count() FROM tab WHERE hasToken(val, 'foo');
SELECT count() FROM tab WHERE hasToken(val, 'FOO');
SELECT count() FROM tab WHERE hasToken(val, 'bar');
-- New parts (with index): postprocessed needle used for index lookup.
SELECT count() FROM tab WHERE hasToken(val, 'baz');
SELECT count() FROM tab WHERE hasToken(val, 'QUX');
SELECT count() FROM tab WHERE hasToken(val, 'qux');
SELECT count() FROM tab WHERE hasToken(val, 'xyz');

SYSTEM START MERGES tab;
DROP TABLE tab;

SELECT '8. Negative tests.';

SELECT '- The postprocessor expression must reference the index column';
CREATE TABLE tab
(
    id UInt64,
    val String,
    other_str String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = lower(other_str))
)
ENGINE = MergeTree ORDER BY tuple();  -- { serverError UNKNOWN_IDENTIFIER }

SELECT '- The postprocessor expression must be a function, not an identifier';
CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = val)
)
ENGINE = MergeTree ORDER BY tuple();  -- { serverError INCORRECT_QUERY }

SELECT '- The postprocessor expression must return String';
CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = length(val))
)
ENGINE = MergeTree ORDER BY tuple();  -- { serverError INCORRECT_QUERY }

SELECT '- The postprocessor must not contain non-deterministic functions';
CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = concat(val, toString(rand())))
)
ENGINE = MergeTree ORDER BY tuple();  -- { serverError INCORRECT_QUERY }

SELECT '- The postprocessor must not contain arrayJoin';
CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = arrayJoin(array(val)))
)
ENGINE = MergeTree ORDER BY tuple();  -- { serverError INCORRECT_QUERY }

DROP TABLE IF EXISTS tab;

SELECT '9. Stop-word postprocessor: empty-mapped tokens must never match vacuously.';

CREATE TABLE tab
(
    id  UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = if(val = 'the', '', val))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, 'hello world'), (2, 'foo bar'), (3, 'the quick');

-- 'the' maps to empty via postprocessor; must return 0, not 3 (vacuously true).
SELECT count() FROM tab WHERE hasToken(val, 'the');
-- Non-stop-word tokens are unaffected.
SELECT count() FROM tab WHERE hasToken(val, 'hello');
SELECT count() FROM tab WHERE hasToken(val, 'quick');

DROP TABLE tab;

SELECT '10. hasAllTokens / hasAnyTokens when all array elements are filtered out.';

CREATE TABLE tab
(
    id  UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = if(val = 'stop', '', val))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, 'hello world'), (2, 'foo bar');

-- All array elements map to empty → must return 0 (not vacuously true).
SELECT count() FROM tab WHERE hasAllTokens(val, ['stop']);
SELECT count() FROM tab WHERE hasAnyTokens(val, ['stop']);

-- Mixed array: stop word is silently dropped; only the surviving token is required.
-- hasAllTokens(['stop', 'hello']) reduces to hasAllTokens(['hello']).
SELECT count() FROM tab WHERE hasAllTokens(val, ['stop', 'hello']);
-- hasAnyTokens(['stop', 'foo']) reduces to hasAnyTokens(['foo']).
SELECT count() FROM tab WHERE hasAnyTokens(val, ['stop', 'foo']);

DROP TABLE tab;

SELECT '11. Index-build path and row-scan path agree when postprocessor drops tokens.';

CREATE TABLE tab (id UInt64, val String) ENGINE = MergeTree ORDER BY id;

SYSTEM STOP MERGES tab;

-- Old parts written before the index was added; these use the row-scan path.
INSERT INTO tab VALUES (1, 'the quick'), (2, 'hello');

ALTER TABLE tab ADD INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = if(val = 'the', '', val));

-- New parts written after the index; these use the index lookup path.
INSERT INTO tab VALUES (3, 'the world'), (4, 'test');

-- Stop word 'the' must return 0 across both old parts (row-scan) and new parts (index).
SELECT count() FROM tab WHERE hasToken(val, 'the');
-- Real tokens must be found consistently regardless of which path is used.
SELECT count() FROM tab WHERE hasToken(val, 'hello');  -- row 2, old part (row-scan)
SELECT count() FROM tab WHERE hasToken(val, 'test');   -- row 4, new part (index)
-- Rows containing 'the' as a stop word are still indexed for their other tokens.
SELECT count() FROM tab WHERE hasToken(val, 'quick');  -- row 1, old part (row-scan)
SELECT count() FROM tab WHERE hasToken(val, 'world');  -- row 3, new part (index)

SYSTEM START MERGES tab;
DROP TABLE tab;

SELECT '12. Array tokenizer + postprocessor: has() / hasAll() / hasAny() bypass the postprocessor.';
-- The array tokenizer stores raw elements; the postprocessor is not applied to them.
-- has/hasAll/hasAny compare against the raw stored elements exactly.
-- The postprocessor is irrelevant for these functions.

CREATE TABLE tab
(
    id UInt64,
    val Array(String),
    INDEX idx(val) TYPE text(tokenizer = 'array', postprocessor = lower(val))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, ['Foo']), (2, ['BAR']), (3, ['baz']);

SELECT count() FROM tab WHERE has(val, 'Foo');   -- 1: exact match 'Foo'
SELECT count() FROM tab WHERE has(val, 'BAR');   -- 1: exact match 'BAR'
SELECT count() FROM tab WHERE has(val, 'foo');   -- 0: 'foo' ≠ 'Foo', postprocessor not applied
SELECT count() FROM tab WHERE has(val, 'xyz');   -- 0

DROP TABLE tab;

SELECT '13. String tokenizer + non-commutative postprocessor: row-scan matches index.';
-- The postprocessor strips the suffix 'ing' from each token (token-level operation).
-- Applying the postprocessor to the whole haystack string ('running walking') gives
-- 'running walking' (no match at end), not ['runn', 'walk']. The rewrite to
-- has(arrayMap(pp, splitByNonAlpha(val)), pp(needle)) ensures correctness on both paths.

CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = replaceRegexpAll(val, 'ing$', ''))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, 'running walking'), (2, 'cat dog');

-- 'running' → strip 'ing' → 'runn'; searching 'running' → 'runn' → found in row 1.
SELECT count() FROM tab WHERE hasToken(val, 'running');  -- 1
-- 'walking' → strip 'ing' → 'walk'; searching 'walking' → 'walk' → found in row 1.
SELECT count() FROM tab WHERE hasToken(val, 'walking');  -- 1
-- 'cat' → no suffix → 'cat'; found in row 2.
SELECT count() FROM tab WHERE hasToken(val, 'cat');      -- 1
-- 'run' → no suffix → 'run'; index stores 'runn', not 'run' → not found.
SELECT count() FROM tab WHERE hasToken(val, 'run');      -- 0
-- Multi-token: both tokens must match after postprocessor.
SELECT count() FROM tab WHERE hasAllTokens(val, 'running walking');  -- 1
SELECT count() FROM tab WHERE hasAllTokens(val, 'running cat');      -- 0

DROP TABLE tab;

SELECT '14. Partially materialized index + postprocessor: haystack is not postprocessed on row-scan.';

-- Old parts use row-level scan. The postprocessor is applied to the needle only,
-- never to the haystack. When old-part data is uppercase and the postprocessor lowercases
-- tokens, the row-scan cannot match: hasToken('FOO', lower('FOO')) = hasToken('FOO', 'foo') = 0.
-- New parts have the index: it stores lower('FOO')='foo', and lower('FOO')='foo' is the
-- lookup key, so row 3 is found. The count 1 (not 2) proves the haystack is untouched.

CREATE TABLE tab (id UInt64, val String) ENGINE = MergeTree ORDER BY id;

SYSTEM STOP MERGES tab;

INSERT INTO tab VALUES (1, 'FOO'), (2, 'BAR');  -- old parts: no index, uppercase data

ALTER TABLE tab ADD INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = lower(val));

INSERT INTO tab VALUES (3, 'FOO'), (4, 'BAR');  -- new parts: with index, same data

-- Old part row-scan: hasToken('FOO', 'foo') = 0. New part index: lower('FOO')='foo' found → 1. Total: 1.
SELECT count() FROM tab WHERE hasToken(val, 'FOO');  -- 1
SELECT count() FROM tab WHERE hasToken(val, 'BAR');  -- 1
SELECT count() FROM tab WHERE hasToken(val, 'xyz');  -- 0

SYSTEM START MERGES tab;
DROP TABLE tab;

SELECT '15. Partially materialized index + non-trivial postprocessor: postprocessed needle vs. raw haystack token.';

-- When the postprocessor significantly transforms tokens (here: strips the suffix "ing"),
-- the postprocessed needle no longer matches the raw token in an unindexed part.
-- Old parts: hasToken('running', postprocessor('running')) = hasToken('running', 'runn') = 0.
-- New parts: index stores 'runn' (from 'running') and looks up postprocessor('running')='runn' → found.
-- Token unaffected by postprocessor ('cat'): old row-scan also matches → total is 2.

CREATE TABLE tab (id UInt64, val String) ENGINE = MergeTree ORDER BY id;

SYSTEM STOP MERGES tab;

INSERT INTO tab VALUES (1, 'running'), (2, 'cat');  -- old parts: no index

ALTER TABLE tab ADD INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = replaceRegexpAll(val, 'ing$', ''));

INSERT INTO tab VALUES (3, 'running'), (4, 'cat');  -- new parts: with index

-- 'running' → postprocessor → 'runn'. Old row-scan: hasToken('running', 'runn') = 0. New index: 'runn' found → 1. Total: 1.
SELECT count() FROM tab WHERE hasToken(val, 'running');  -- 1
-- 'cat' is unchanged by the postprocessor. Old row-scan: hasToken('cat', 'cat') = 1. New index: 'cat' found → 1. Total: 2.
SELECT count() FROM tab WHERE hasToken(val, 'cat');      -- 2
SELECT count() FROM tab WHERE hasToken(val, 'xyz');      -- 0

SYSTEM START MERGES tab;
DROP TABLE tab;

SELECT '16. Legacy predicates (hasPhrase / startsWith / endsWith) work correctly with a postprocessor.';

CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = replaceRegexpAll(val, 'ing$', ''))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, 'running walking'), (2, 'cat dog');

-- Row-level semantics hold and the index is used in Hint mode.
SELECT count() FROM tab WHERE hasPhrase(val, 'running walking');  -- 1
SELECT count() FROM tab WHERE startsWith(val, 'running');         -- 1
SELECT count() FROM tab WHERE endsWith(val, 'walking');           -- 1

DROP TABLE tab;

SELECT '17. Array tokenizer + preprocessor: has() / hasAll() / hasAny() bypass the preprocessor.';

CREATE TABLE tab
(
    id UInt64,
    val Array(String),
    INDEX idx(val) TYPE text(tokenizer = 'array', preprocessor = lower(val))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, ['Foo']), (2, ['BAR']), (3, ['baz']);

-- The index stores preprocessed elements, but these functions use raw array semantics.
SELECT count() FROM tab WHERE has(val, 'Foo');         -- 1
SELECT count() FROM tab WHERE has(val, 'foo');         -- 0
SELECT count() FROM tab WHERE hasAll(val, ['BAR']);    -- 1
SELECT count() FROM tab WHERE hasAll(val, ['bar']);    -- 0
SELECT count() FROM tab WHERE hasAny(val, ['baz']);    -- 1
SELECT count() FROM tab WHERE hasAny(val, ['BAZ']);    -- 0

DROP TABLE tab;

SELECT '18. startsWith / endsWith use hint with postprocessor when normalized tokens survive.';

CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = replaceRegexpAll(val, 'ing$', ''))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, 'running walking'), (2, 'cat dog');

-- Row-level results are correct.
SELECT count() FROM tab WHERE startsWith(val, 'running walking');  -- 1
SELECT count() FROM tab WHERE endsWith(val, 'cat dog');            -- 1

-- Index hint is used: the virtual column __text_index appears in the plan.
SELECT trimLeft(explain) FROM
(EXPLAIN indexes = 1, actions = 1 SELECT count() FROM tab WHERE startsWith(val, 'running walking'))
WHERE explain LIKE '%__text_index%';

SELECT trimLeft(explain) FROM
(EXPLAIN indexes = 1, actions = 1 SELECT count() FROM tab WHERE endsWith(val, 'cat dog'))
WHERE explain LIKE '%__text_index%';

DROP TABLE tab;

SELECT '19. startsWith / endsWith do not use hint when postprocessor drops all hint tokens.';

CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = if(val = 'the', '', val))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, 'the quick fox');

-- Row-level result is correct.
SELECT count() FROM tab WHERE startsWith(val, 'the quick');  -- 1

-- Postprocessor maps 'the' to '' → normalized prefix token list is empty → index not used.
SELECT trimLeft(explain) FROM
(EXPLAIN indexes = 1, actions = 1 SELECT count() FROM tab WHERE startsWith(val, 'the quick'))
WHERE explain LIKE '%__text_index%';

DROP TABLE tab;

SELECT '20. startsWith / endsWith stay correct across mixed indexed and non-indexed parts.';

CREATE TABLE tab
(
    id UInt64,
    val String
)
ENGINE = MergeTree ORDER BY id;

SYSTEM STOP MERGES tab;

INSERT INTO tab VALUES (1, 'running walking');

ALTER TABLE tab ADD INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = replaceRegexpAll(val, 'ing$', ''));

INSERT INTO tab VALUES (2, 'running walking');

SELECT count() FROM tab WHERE startsWith(val, 'running walking');  -- 2
SELECT count() FROM tab WHERE endsWith(val, 'running walking');    -- 2

ALTER TABLE tab MATERIALIZE INDEX idx;

SELECT count() FROM tab WHERE startsWith(val, 'running walking');  -- 2
SELECT count() FROM tab WHERE endsWith(val, 'running walking');    -- 2

SYSTEM START MERGES tab;
DROP TABLE tab;
