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

SELECT '4. Regex-based postprocessor.';

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

SELECT '5. Timestamp removal: postprocessor uses parseDateTimeOrNull to drop timestamp tokens.';

-- Log lines are split by whitespace (splitByString default tokenizer), so the ISO timestamp
-- becomes a single token per line. The postprocessor uses parseDateTimeOrNull with an explicit
-- format to detect and drop timestamp-format tokens (mapping them to '').
-- Non-timestamp tokens are kept unchanged.

CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(
        tokenizer    = 'splitByString',
        postprocessor = if(isNotNull(parseDateTimeOrNull(val, '%Y-%m-%dT%H:%i:%S')), '', val)
    )
)
ENGINE = MergeTree ORDER BY id;
SYSTEM STOP MERGES tab;

INSERT INTO tab VALUES
    (1, '2024-01-15T10:23:45 ERROR connection failed'),
    (2, '2024-01-15T10:23:46 INFO server started'),
    (3, '2024-01-15T10:23:47 ERROR disk full');

-- Searching by message content finds rows.
SELECT count() FROM tab WHERE hasToken(val, 'ERROR');                -- 2
SELECT count() FROM tab WHERE hasToken(val, 'connection');           -- 1
SELECT count() FROM tab WHERE hasToken(val, 'server');               -- 1
-- The timestamp token maps to '' via the postprocessor; the index never stored it.
SELECT count() FROM tab WHERE hasToken(val, '2024-01-15T10:23:45'); -- 0

-- The index stores only message-level tokens.
SELECT token, cardinality
FROM mergeTreeTextIndex(currentDatabase(), tab, idx)
ORDER BY token;

SYSTEM START MERGES tab;
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

SELECT '7. Stop-word filtering postprocessor.';

-- Tokens that map to empty string are excluded from the index.
-- Searching a needle that contains a stop word correctly ignores it.
CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = if(val = 'the', '', val))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, 'hello world'), (2, 'foo bar'), (3, 'the the');

-- 'hello' and 'world' are stored normally; 'foo' and 'bar' too.
SELECT count() FROM tab WHERE hasToken(val, 'hello');
SELECT count() FROM tab WHERE hasToken(val, 'world');
-- When 'the' appears in the needle string, it is filtered from the search tokens.
-- hasAllTokens(val, 'hello the world') is equivalent to hasAllTokens(val, 'hello world').
SELECT count() FROM tab WHERE hasAllTokens(val, 'hello world');
SELECT count() FROM tab WHERE hasAllTokens(val, 'hello the world');
-- hasPhrase is hint-only and not postprocessed at row level: a phrase whose tokens are all dropped by
-- the postprocessor ('the the') must still match via the row scan instead of pruning every granule.
SELECT count() FROM tab WHERE hasPhrase(val, 'the the');
SELECT count() FROM tab WHERE hasPhrase(val, 'foo bar');
-- Tokens present but not in this adjacent order: granule kept, row-level rejects.
SELECT count() FROM tab WHERE hasPhrase(val, 'world hello');

DROP TABLE tab;

SELECT '8. Stop-word postprocessor: empty-mapped tokens must never match vacuously.';

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
-- hasTokenOrNull is NOT postprocessed at row level (unlike hasToken), so the index must not be used:
-- the dropped 'the' is still found literally in 'the quick'. Contrast hasToken('the') = 0 with this = 1.
SELECT count() FROM tab WHERE hasTokenOrNull(val, 'the');
SELECT count() FROM tab WHERE hasTokenOrNull(val, 'quick');
SELECT count() FROM tab WHERE hasTokenOrNull(val, 'xyz');

DROP TABLE tab;

SELECT '9. hasAllTokens / hasAnyTokens when all array elements are filtered out.';

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

-- An OR keeps the granule alive so the empty search is evaluated directly (granule pruning no longer
-- masks it); the empty search must still match nothing, so only id = 1 qualifies. Direct read previously
-- leaked an always-true virtual column here and returned all rows.
SELECT count() FROM tab WHERE hasAllTokens(val, ['stop']) OR id = 1;
SELECT count() FROM tab WHERE hasAnyTokens(val, ['stop']) OR id = 1;

DROP TABLE tab;

SELECT '10. Index-build path and row-scan path agree when postprocessor drops tokens.';

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

SELECT '11. Array tokenizer + postprocessor: has() / hasAll() / hasAny() use postprocessor via hint mode.';
-- The index stores postprocessed (lower-cased) elements.
-- has/hasAll/hasAny apply the postprocessor to the needle for the index lookup (hint mode),
-- then re-evaluate the original predicate at row level.

CREATE TABLE tab
(
    id UInt64,
    val Array(String),
    INDEX idx(val) TYPE text(tokenizer = 'array', postprocessor = lower(val))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, ['Foo']), (2, ['BAR']), (3, ['baz']);

SELECT count() FROM tab WHERE has(val, 'Foo');   -- 1: index finds 'foo', row-level has(['Foo'], 'Foo') → true
SELECT count() FROM tab WHERE has(val, 'BAR');   -- 1: index finds 'bar', row-level has(['BAR'], 'BAR') → true
SELECT count() FROM tab WHERE has(val, 'foo');   -- 0: index finds 'foo' (hint), row-level has(['Foo'], 'foo') → false
SELECT count() FROM tab WHERE has(val, 'xyz');   -- 0

DROP TABLE tab;

SELECT '12. String tokenizer + non-commutative postprocessor: row-scan matches index.';
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

SELECT '13. Partially materialized index.';

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

SELECT '14. Partially materialized index + postprocessor: haystack is postprocessed on row-scan too.';

-- The postprocessor is applied to the haystack on the row-scan path as well, so unindexed (old) parts
-- match the same rows as indexed (new) parts. lower('FOO')='foo' on both paths, and the needle is
-- lowered to 'foo', so each needle matches both its old and new row: count 2 (independent of whether
-- the index is read).

CREATE TABLE tab (id UInt64, val String) ENGINE = MergeTree ORDER BY id;

SYSTEM STOP MERGES tab;

INSERT INTO tab VALUES (1, 'FOO'), (2, 'BAR');  -- old parts: no index, uppercase data

ALTER TABLE tab ADD INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = lower(val));

INSERT INTO tab VALUES (3, 'FOO'), (4, 'BAR');  -- new parts: with index, same data

-- Old row-scan and new index both postprocess to 'foo'/'bar', so each needle matches both rows.
SELECT count() FROM tab WHERE hasToken(val, 'FOO');  -- 2
SELECT count() FROM tab WHERE hasToken(val, 'BAR');  -- 2
SELECT count() FROM tab WHERE hasToken(val, 'xyz');  -- 0

SYSTEM START MERGES tab;
DROP TABLE tab;

SELECT '15. Partially materialized index + non-trivial postprocessor: haystack postprocessed on row-scan.';

-- A postprocessor that significantly transforms tokens (here: strips the suffix "ing") is applied to
-- the haystack on the row-scan path too, so an unindexed part matches the same rows as an indexed one.
-- 'running' → 'runn' on both paths, and the needle is postprocessed to 'runn', so both rows match.

CREATE TABLE tab (id UInt64, val String) ENGINE = MergeTree ORDER BY id;

SYSTEM STOP MERGES tab;

INSERT INTO tab VALUES (1, 'running'), (2, 'cat');  -- old parts: no index

ALTER TABLE tab ADD INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = replaceRegexpAll(val, 'ing$', ''));

INSERT INTO tab VALUES (3, 'running'), (4, 'cat');  -- new parts: with index

-- 'running' → 'runn' on both row-scan and index paths → both rows match: 2.
SELECT count() FROM tab WHERE hasToken(val, 'running');  -- 2
-- 'cat' is unchanged by the postprocessor → both rows match: 2.
SELECT count() FROM tab WHERE hasToken(val, 'cat');      -- 2
SELECT count() FROM tab WHERE hasToken(val, 'xyz');      -- 0

SYSTEM START MERGES tab;
DROP TABLE tab;

DROP TABLE IF EXISTS tab;
