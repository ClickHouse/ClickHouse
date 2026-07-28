-- Tests the postprocessor argument in text indexes: search predicates route their needles through
-- the postprocessor (has/hasAll/hasAny, hasAllTokens/hasAnyTokens, startsWith/endsWith/hasPhrase,
-- IN, equals, mapContains*).

DROP TABLE IF EXISTS tab;

SELECT '1. Array tokenizer + postprocessor: has() / hasAll() / hasAny() use postprocessor via hint mode.';
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

SELECT '2. hasAll / hasAny with non-array tokenizer + postprocessor: needle elements go through postprocessor.';
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

SELECT '3. Predicates hasPhrase / startsWith / endsWith work correctly with a postprocessor.';

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

SELECT '4. startsWith / endsWith use hint with postprocessor when normalized tokens survive.';

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

SELECT '5. startsWith stays correct when the postprocessor maps all hint tokens to empty.';

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

SELECT '6. val IN (...) routes set elements through the postprocessor.';
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

SELECT '7. String column + array tokenizer + postprocessor: equals / hasAllTokens use postprocessor via hint mode.';
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

SELECT '8. Map column + postprocessor: mapContainsKey / mapContainsKeyLike normalize the needle.';

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

SELECT '9. Map column + postprocessor: mapContainsValue / mapContainsValueLike normalize the needle.';

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

SELECT '10. Map column + array tokenizer + postprocessor: mapContains* use postprocessor via hint mode.';
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

SELECT '11. sparseGrams + postprocessor: needle compaction stays sound (no false positives in Exact direct read).';
-- sparseGrams compaction drops a shorter gram covered by a longer one, relying on the index guaranteeing
-- that a document with the longer gram also has the shorter one. A length-changing postprocessor (here:
-- vowel removal) breaks that invariant, so the hasAllTokens lookup must require every postprocessed needle
-- token, not just the longest. Needle 'utfub' -> grams {utf,tfu,fub,tfub,utfub} -> postprocessed {tf,fb,tfb}.
-- 'tf' and 'fb' are substrings of 'tfb', so unsound compaction would require only 'tfb' and over-match.

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id UInt64, val String) ENGINE = MergeTree ORDER BY id;
ALTER TABLE tab ADD INDEX idx(val) TYPE text(tokenizer = sparseGrams(3, 5), postprocessor = replaceRegexpAll(val, '[aeiou]', ''));

-- Postprocessed token sets: 'etfb' -> {tf,tfb}, 'fbtfb' -> {fbt,btf,fbtf,tfb,fbtfb}, 'tafabu' -> {tf,f,fb,b}.
-- None contains all of {tf,fb,tfb}, so no row satisfies hasAllTokens(val, 'utfub').
INSERT INTO tab VALUES (1, 'etfb'), (2, 'fbtfb'), (3, 'tafabu');

SET use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

-- 'etfb' lacks 'fb', 'fbtfb' lacks 'tf'/'fb', 'tafabu' lacks 'tfb' -> 0 (unsound compaction returns 2).
SELECT count() FROM tab WHERE hasAllTokens(val, 'utfub');   -- 0
-- Control: needle 'etf' -> {tf}; 'etfb' and 'tafabu' contain 'tf', 'fbtfb' does not -> 2.
SELECT count() FROM tab WHERE hasAllTokens(val, 'etf');     -- 2

DROP TABLE IF EXISTS tab;
