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
