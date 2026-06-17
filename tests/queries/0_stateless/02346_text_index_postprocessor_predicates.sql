-- Tests the postprocessor argument in text indexes: legacy predicates (hasPhrase /
-- startsWith / endsWith), val IN (...), Map columns, the array tokenizer, ngrams and
-- the independence from query_plan_direct_read_from_text_index, plus negative tests.
-- Split from 02346_text_index_postprocessor.sql to keep each run under the CI time limit.

DROP TABLE IF EXISTS tab;

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

SELECT '30. hasToken / hasAnyTokens / hasAllTokens results are independent of query_plan_direct_read_from_text_index.';
-- The postprocessor is applied to the haystack at row level, so each query returns the same count
-- whether the index is read directly (=1) or the rows are scanned (=0). Every pair below must match.

CREATE TABLE tab
(
    id  UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = lower(val))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, 'Hello World'), (2, 'FOO bar'), (3, 'baz QUX'), (4, 'Hello FOO'), (5, 'WORLD baz');

SELECT count() FROM tab WHERE hasToken(val, 'HELLO') SETTINGS query_plan_direct_read_from_text_index = 1;             -- 2
SELECT count() FROM tab WHERE hasToken(val, 'HELLO') SETTINGS query_plan_direct_read_from_text_index = 0;             -- 2
SELECT count() FROM tab WHERE hasToken(val, 'qux') SETTINGS query_plan_direct_read_from_text_index = 1;               -- 1
SELECT count() FROM tab WHERE hasToken(val, 'qux') SETTINGS query_plan_direct_read_from_text_index = 0;               -- 1
SELECT count() FROM tab WHERE hasToken(val, 'xyz') SETTINGS query_plan_direct_read_from_text_index = 1;               -- 0
SELECT count() FROM tab WHERE hasToken(val, 'xyz') SETTINGS query_plan_direct_read_from_text_index = 0;               -- 0
SELECT count() FROM tab WHERE hasAnyTokens(val, ['HELLO', 'qux']) SETTINGS query_plan_direct_read_from_text_index = 1; -- 3
SELECT count() FROM tab WHERE hasAnyTokens(val, ['HELLO', 'qux']) SETTINGS query_plan_direct_read_from_text_index = 0; -- 3
SELECT count() FROM tab WHERE hasAllTokens(val, 'Hello World') SETTINGS query_plan_direct_read_from_text_index = 1;    -- 1
SELECT count() FROM tab WHERE hasAllTokens(val, 'Hello World') SETTINGS query_plan_direct_read_from_text_index = 0;    -- 1
SELECT count() FROM tab WHERE hasAllTokens(val, ['hello', 'foo']) SETTINGS query_plan_direct_read_from_text_index = 1; -- 1
SELECT count() FROM tab WHERE hasAllTokens(val, ['hello', 'foo']) SETTINGS query_plan_direct_read_from_text_index = 0; -- 1
SELECT count() FROM tab WHERE hasAnyTokens(val, ['xyz', 'abc']) SETTINGS query_plan_direct_read_from_text_index = 1;   -- 0
SELECT count() FROM tab WHERE hasAnyTokens(val, ['xyz', 'abc']) SETTINGS query_plan_direct_read_from_text_index = 0;   -- 0

DROP TABLE tab;

-- A boundary-changing postprocessor (concat appends ' x', emitting a separator): the index stores whole
-- tokens like 'foo x'. On the row-scan fallback the haystack is the final-token array, which must be
-- matched with 'array' semantics rather than re-split into 'foo','x'. Results stay independent of dr.
CREATE TABLE tab
(
    id  UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = concat(val, ' x'))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, 'foo'), (2, 'bar');

SELECT count() FROM tab WHERE hasAnyTokens(val, 'foo') SETTINGS query_plan_direct_read_from_text_index = 1; -- 1
SELECT count() FROM tab WHERE hasAnyTokens(val, 'foo') SETTINGS query_plan_direct_read_from_text_index = 0; -- 1
SELECT count() FROM tab WHERE hasAllTokens(val, 'foo') SETTINGS query_plan_direct_read_from_text_index = 1; -- 1
SELECT count() FROM tab WHERE hasAllTokens(val, 'foo') SETTINGS query_plan_direct_read_from_text_index = 0; -- 1
SELECT count() FROM tab WHERE hasAnyTokens(val, 'zzz') SETTINGS query_plan_direct_read_from_text_index = 1; -- 0
SELECT count() FROM tab WHERE hasAnyTokens(val, 'zzz') SETTINGS query_plan_direct_read_from_text_index = 0; -- 0

DROP TABLE tab;

SELECT '31. Array column + non-array tokenizer: each element is tokenized before the postprocessor.';
-- Index build runs splitByNonAlpha on every array element, so 'Foo Bar' -> ['Foo','Bar'] -> lower.
-- The row-level fallback must do the same; using the element verbatim ('foo bar') would miss
-- single-token needles. Each pair below must agree with direct read on (=1) and off (=0).

CREATE TABLE tab
(
    id UInt64,
    val Array(String),
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = lower(val))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, ['Foo Bar', 'Baz']), (2, ['hello world']);

SELECT count() FROM tab WHERE hasAllTokens(val, ['foo']) SETTINGS query_plan_direct_read_from_text_index = 1;        -- 1
SELECT count() FROM tab WHERE hasAllTokens(val, ['foo']) SETTINGS query_plan_direct_read_from_text_index = 0;        -- 1
SELECT count() FROM tab WHERE hasAllTokens(val, ['foo', 'bar']) SETTINGS query_plan_direct_read_from_text_index = 1; -- 1
SELECT count() FROM tab WHERE hasAllTokens(val, ['foo', 'bar']) SETTINGS query_plan_direct_read_from_text_index = 0; -- 1
SELECT count() FROM tab WHERE hasAllTokens(val, ['baz']) SETTINGS query_plan_direct_read_from_text_index = 1;        -- 1
SELECT count() FROM tab WHERE hasAllTokens(val, ['baz']) SETTINGS query_plan_direct_read_from_text_index = 0;        -- 1
SELECT count() FROM tab WHERE hasAnyTokens(val, ['world']) SETTINGS query_plan_direct_read_from_text_index = 1;      -- 1
SELECT count() FROM tab WHERE hasAnyTokens(val, ['world']) SETTINGS query_plan_direct_read_from_text_index = 0;      -- 1
SELECT count() FROM tab WHERE hasAllTokens(val, ['xyz']) SETTINGS query_plan_direct_read_from_text_index = 1;        -- 0
SELECT count() FROM tab WHERE hasAllTokens(val, ['xyz']) SETTINGS query_plan_direct_read_from_text_index = 0;        -- 0

DROP TABLE tab;

SELECT '32. Array column + non-array tokenizer: unmaterialized (row-scan) and indexed parts agree.';

CREATE TABLE tab (id UInt64, val Array(String)) ENGINE = MergeTree ORDER BY id;

SYSTEM STOP MERGES tab;

INSERT INTO tab VALUES (1, ['Foo Bar']);  -- old part: no index, evaluated by the row-scan fallback

ALTER TABLE tab ADD INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = lower(val));

INSERT INTO tab VALUES (2, ['Foo Bar']);  -- new part: indexed

-- Both rows tokenize to ['Foo','Bar'] -> lower -> ['foo','bar']; each needle must match both parts.
SELECT count() FROM tab WHERE hasAllTokens(val, ['foo']);          -- 2
SELECT count() FROM tab WHERE hasAllTokens(val, ['foo', 'bar']);   -- 2
SELECT count() FROM tab WHERE hasAnyTokens(val, ['bar']);          -- 2
SELECT count() FROM tab WHERE hasAllTokens(val, ['xyz']);          -- 0

SYSTEM START MERGES tab;
DROP TABLE tab;

DROP TABLE IF EXISTS tab;
