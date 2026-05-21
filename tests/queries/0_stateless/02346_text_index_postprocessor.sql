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

INSERT INTO tab VALUES (1, 'hello world'), (2, 'foo bar');

-- 'hello' and 'world' are stored normally; 'foo' and 'bar' too.
SELECT count() FROM tab WHERE hasToken(val, 'hello');
SELECT count() FROM tab WHERE hasToken(val, 'world');
-- When 'the' appears in the needle string, it is filtered from the search tokens.
-- hasAllTokens(val, 'hello the world') is equivalent to hasAllTokens(val, 'hello world').
SELECT count() FROM tab WHERE hasAllTokens(val, 'hello world');
SELECT count() FROM tab WHERE hasAllTokens(val, 'hello the world');

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

SELECT '17. startsWith / endsWith use hint with postprocessor when normalized tokens survive.';

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

DROP TABLE tab;

SELECT '18. startsWith stays correct when the postprocessor maps all hint tokens to empty.';

CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = if(val = 'the', '', val))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, 'the quick fox');

-- The postprocessor maps 'the' to '' so the prefix tokens are all dropped.
-- The row-level startsWith filter still resolves the predicate correctly.
SELECT count() FROM tab WHERE startsWith(val, 'the quick');  -- 1

DROP TABLE tab;

SELECT '19. startsWith / endsWith stay correct across mixed indexed and non-indexed parts.';

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

SELECT '20. val IN (...) routes set elements through the postprocessor.';
-- The bug: tryPrepareSetForTextSearch built set tokens with preprocessor + tokenizer
-- only, ignoring the postprocessor. Index stored 'foo' (postprocessed); a query
-- val IN ('FOO') would search for token 'FOO', miss, prune the granule, and drop
-- the matching row 1. The fix routes set elements through the same stringToTokens
-- helper (preprocessor + tokenizer + postprocessor) used elsewhere.

CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = lower(val))
)
ENGINE = MergeTree ORDER BY id;

-- Rows are stored as-is; the postprocessor only affects index tokens.
INSERT INTO tab VALUES (1, 'FOO'), (2, 'bar'), (3, 'Baz');

-- IN exact-matches the stored value. The granule must NOT be pruned just because
-- the set element differs from the indexed (postprocessed) token form.
SELECT count() FROM tab WHERE val IN ('FOO');         -- 1: row 1 exact match
SELECT count() FROM tab WHERE val IN ('bar');         -- 1: row 2 exact match
SELECT count() FROM tab WHERE val IN ('Baz');         -- 1: row 3 exact match

-- Set element with different case from stored row: row-level IN is false, but the
-- index lookup must still go through the postprocessor and not produce a stale 0.
SELECT count() FROM tab WHERE val IN ('foo');         -- 0: no row equals 'foo' literally
SELECT count() FROM tab WHERE val IN ('xyz');         -- 0

-- Multi-element IN, with elements of different cases. All matching rows must survive.
SELECT count() FROM tab WHERE val IN ('FOO', 'bar');  -- 2: rows 1 and 2

-- NOT IN exercises the same set-token build path.
SELECT count() FROM tab WHERE val NOT IN ('FOO');     -- 2: rows 2 and 3

DROP TABLE tab;

SELECT '-- IN element that the postprocessor maps to empty: index falls back to row-scan.';

CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = if(val = 'the', '', val))
) ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, 'the'), (2, 'cat');

-- 'the' is dropped by the postprocessor (not stored in the index). The set-token
-- builder bails out (empty tokens) so the index is not used; row-scan finds the row.
SELECT count() FROM tab WHERE val IN ('the');           -- 1
SELECT count() FROM tab WHERE val IN ('cat');           -- 1
SELECT count() FROM tab WHERE val IN ('the', 'cat');    -- 2

DROP TABLE tab;

SELECT '21. Array tokenizer + postprocessor: rewrite path matches index for mixed parts.';

-- For indexed parts, index lookup uses postprocessed needle in hint mode, then row-level
-- re-evaluates the original predicate. For non-indexed parts, row-level runs directly.

CREATE TABLE tab (id UInt64, val Array(String)) ENGINE = MergeTree ORDER BY id;
SYSTEM STOP MERGES tab;

INSERT INTO tab VALUES (1, ['Foo']), (2, ['BAR']);  -- old parts: no index

ALTER TABLE tab ADD INDEX idx(val) TYPE text(tokenizer = 'array', postprocessor = lower(val));

INSERT INTO tab VALUES (3, ['Foo']), (4, ['BAR']);  -- new parts: indexed

-- The postprocessor is applied element-wise to the haystack and to the needle.
-- Both 'Foo' and 'foo' normalize to 'foo', so they match the same rows.
SELECT count() FROM tab WHERE hasAllTokens(val, ['Foo']);  -- 2
SELECT count() FROM tab WHERE hasAnyTokens(val, ['BAR']);  -- 2
SELECT count() FROM tab WHERE hasAllTokens(val, ['foo']);  -- 2

SYSTEM START MERGES tab;
DROP TABLE tab;

SELECT '22. Map column + postprocessor: mapContainsKey / mapContainsKeyLike normalize the needle.';

CREATE TABLE tab
(
    id UInt64,
    val Map(String, String),
    INDEX idx(mapKeys(val)) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = lower(mapKeys(val)))
) ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, {'FOO': 'a'}), (2, {'BAR': 'a'});

-- Postprocessor on needle: 'FOO' -> 'foo'; index has 'foo'; row-level matches the literal key.
SELECT count() FROM tab WHERE mapContainsKey(val, 'FOO');         -- 1
-- Index keeps the granule, but the literal key 'foo' is absent from the map.
SELECT count() FROM tab WHERE mapContainsKey(val, 'foo');         -- 0
SELECT count() FROM tab WHERE mapContainsKeyLike(val, '%FOO%');   -- 1

DROP TABLE tab;

SELECT '23. Map column + postprocessor: mapContainsValue / mapContainsValueLike normalize the needle.';

CREATE TABLE tab
(
    id UInt64,
    val Map(String, String),
    INDEX idx(mapValues(val)) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = lower(mapValues(val)))
) ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, {'a': 'FOO'}), (2, {'a': 'BAR'});

SELECT count() FROM tab WHERE mapContainsValue(val, 'FOO');         -- 1
SELECT count() FROM tab WHERE mapContainsValue(val, 'foo');         -- 0
SELECT count() FROM tab WHERE mapContainsValueLike(val, '%FOO%');   -- 1

DROP TABLE tab;

SELECT '24. Map column + array tokenizer + postprocessor: mapContains* use postprocessor via hint mode.';
-- Index stores postprocessed (lower-cased) keys/values. Lookup applies postprocessor to the needle
-- (hint mode) and re-evaluates the original predicate at row level.

CREATE TABLE tab
(
    id UInt64,
    val Map(String, String),
    INDEX idx(mapKeys(val)) TYPE text(tokenizer = 'array', postprocessor = lower(mapKeys(val)))
) ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, {'Foo': 'a'}), (2, {'BAR': 'b'});

-- Index finds 'foo'/'bar' (postprocessed needle); row-level checks literal key.
SELECT count() FROM tab WHERE mapContainsKey(val, 'Foo');   -- 1
SELECT count() FROM tab WHERE mapContainsKey(val, 'BAR');   -- 1
-- Index finds 'foo' (hint), but row-level mapContainsKey({'Foo': 'a'}, 'foo') → false.
SELECT count() FROM tab WHERE mapContainsKey(val, 'foo');   -- 0

DROP TABLE tab;

CREATE TABLE tab
(
    id UInt64,
    val Map(String, String),
    INDEX idx(mapValues(val)) TYPE text(tokenizer = 'array', postprocessor = lower(mapValues(val)))
) ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, {'a': 'Foo'}), (2, {'b': 'BAR'});

SELECT count() FROM tab WHERE mapContainsValue(val, 'Foo');   -- 1
SELECT count() FROM tab WHERE mapContainsValue(val, 'BAR');   -- 1
SELECT count() FROM tab WHERE mapContainsValue(val, 'foo');   -- 0

DROP TABLE tab;

SELECT '25. String column + array tokenizer + postprocessor: equals / hasAllTokens use postprocessor via hint mode.';
-- Index stores postprocessed (lower-cased) values. Lookup applies postprocessor to the needle
-- (hint mode) and re-evaluates the original predicate at row level.

CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'array', postprocessor = lower(val))
) ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, 'Foo'), (2, 'BAR'), (3, 'baz');

-- equals uses hint mode: index postprocesses needle, row-level checks literal value.
SELECT count() FROM tab WHERE val = 'Foo';                        -- 1
SELECT count() FROM tab WHERE val = 'BAR';                        -- 1
SELECT count() FROM tab WHERE val = 'baz';                        -- 1
-- Index finds 'foo' (hint), but row-level val = 'foo' on val='Foo' → false.
SELECT count() FROM tab WHERE val = 'foo';                        -- 0
-- hasToken / hasAllTokens: postprocessor applied to both haystack and needle → case-insensitive match.
SELECT count() FROM tab WHERE hasToken(val, 'Foo');               -- 1
SELECT count() FROM tab WHERE hasToken(val, 'foo');               -- 1
SELECT count() FROM tab WHERE hasAllTokens(val, 'Foo');           -- 1
SELECT count() FROM tab WHERE hasAllTokens(val, 'foo');           -- 1
SELECT count() FROM tab WHERE val IN ('Foo', 'BAR');              -- 2
SELECT count() FROM tab WHERE val IN ('foo', 'bar');              -- 0

DROP TABLE tab;

SELECT '26. hasTokenOrNull: stop-word postprocessor is honored consistently across index and row-scan paths.';
-- Mirrors test 10 for hasTokenOrNull. hasTokenOrNull is treated like hasToken in the optimization
-- (Exact direct-read mode + needApplyPostprocessor), so the rewrite applies on both materialized
-- and non-materialized parts. Without the fix, granule pruning would diverge from row-level
-- evaluation when the postprocessor maps the needle to empty.

CREATE TABLE tab (id UInt64, val String) ENGINE = MergeTree ORDER BY id;
SYSTEM STOP MERGES tab;

-- Old parts: row-scan path (no index materialized).
INSERT INTO tab VALUES (1, 'the quick'), (2, 'hello world');

ALTER TABLE tab ADD INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = if(val = 'the', '', val));

-- New parts: indexed path.
INSERT INTO tab VALUES (3, 'the apple'), (4, 'foo bar');

-- Stop word 'the' must return 0 across both old (row-scan) and new (index) parts.
SELECT count() FROM tab WHERE hasTokenOrNull(val, 'the');
-- Real tokens are found consistently regardless of the path used.
SELECT count() FROM tab WHERE hasTokenOrNull(val, 'apple');  -- row 3, new part (index)
SELECT count() FROM tab WHERE hasTokenOrNull(val, 'hello');  -- row 2, old part (row-scan)
-- Invalid needle: hasTokenOrNull returns NULL, count() does not count NULLs.
SELECT count() FROM tab WHERE hasTokenOrNull(val, '!@#');
-- NULL semantics are preserved by the optimization: invalid needles return NULL on every row,
-- valid-but-missing needles return 0 — granule pruning never converts a 0 into a NULL.
SELECT countIf(hasTokenOrNull(val, '!@#') IS NULL) FROM tab;       -- 4 (all rows NULL)
SELECT countIf(hasTokenOrNull(val, 'missing') = 0) FROM tab;       -- 4 (all rows 0, no match)
SELECT countIf(hasTokenOrNull(val, 'missing') IS NULL) FROM tab;   -- 0

SYSTEM START MERGES tab;
DROP TABLE tab;

SELECT '27. Negative tests.';

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

SELECT '- A postprocessor that produces a token containing separator characters throws BAD_ARGUMENTS at query time';
CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = concat(val, ' x'))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, 'foo');

SELECT count() FROM tab WHERE hasToken(val, 'foo');  -- { serverError BAD_ARGUMENTS }

DROP TABLE tab;

SELECT '28. ngrams tokenizer + postprocessor: postprocessed tokens are not re-tokenized.';
-- Before the fix, postprocessed tokens were fed back through addDocument, re-running the tokenizer.
-- A 3-char ngram truncated to 2 chars is shorter than n=3, so re-tokenization produces nothing
-- and the index is empty, causing false negatives on all hasToken queries.

CREATE TABLE tab
(
    id  UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = ngrams(3), postprocessor = substring(val, 1, 2))
)
ENGINE = MergeTree ORDER BY id;

SYSTEM STOP MERGES tab;
INSERT INTO tab VALUES (1, 'hello'), (2, 'world');

-- 'hello' → ngrams(3) → ['hel','ell','llo'] → substring(1,2) → ['he','el','ll']
-- 'world' → ngrams(3) → ['wor','orl','rld'] → substring(1,2) → ['wo','or','rl']
SELECT token, cardinality
FROM mergeTreeTextIndex(currentDatabase(), tab, idx)
ORDER BY token;

SELECT count() FROM tab WHERE hasToken(val, 'hello');  -- 1
SELECT count() FROM tab WHERE hasToken(val, 'world');  -- 1
SELECT count() FROM tab WHERE hasToken(val, 'xyz');    -- 0

SYSTEM START MERGES tab;
DROP TABLE tab;

SELECT '29. hasAll / hasAny with non-array tokenizer + postprocessor: needle elements go through postprocessor.';
-- The index stores postprocessed (lower-cased) tokens. Before the fix, hasAll / hasAny built
-- lookup tokens without the postprocessor, so 'FOO' was looked up instead of 'foo', the granule
-- was falsely pruned, and matching rows were dropped.

CREATE TABLE tab
(
    id  UInt64,
    val Array(String),
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = lower(val))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, ['FOO', 'BAR']), (2, ['baz']);

-- Index stores 'foo', 'bar' for row 1 and 'baz' for row 2.
-- The postprocessor normalises needle elements for index lookup (hint mode);
-- row-level hasAll/hasAny still compares needle and haystack literally.
SELECT count() FROM tab WHERE hasAll(val, ['FOO', 'BAR']);   -- 1: exact match at row level
SELECT count() FROM tab WHERE hasAll(val, ['foo', 'bar']);   -- 0: row-level match fails
SELECT count() FROM tab WHERE hasAll(val, ['Foo', 'Bar']);   -- 0: row-level match fails
SELECT count() FROM tab WHERE hasAny(val, ['FOO']);          -- 1: exact match at row level
SELECT count() FROM tab WHERE hasAny(val, ['Foo']);          -- 0: row-level match fails
SELECT count() FROM tab WHERE hasAny(val, ['xyz']);          -- 0

DROP TABLE tab;

DROP TABLE IF EXISTS tab;
