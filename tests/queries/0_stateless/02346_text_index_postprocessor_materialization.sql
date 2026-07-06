-- Tags: no-fasttest
-- Tag no-fasttest -- section 14 uses lowerUTF8, which is only available in builds with ICU.

-- Tests the postprocessor argument in text indexes: index build, (partial) materialization and direct
-- reads stay consistent with the row-scan fallback, including ngrams/sparseGrams tokenizer specifics.

DROP TABLE IF EXISTS tab;

SELECT '1. Index-build path and row-scan path agree when postprocessor drops tokens.';

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

SELECT '2. String tokenizer + non-commutative postprocessor: row-scan matches index.';
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

SELECT '3. Partially materialized index.';

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

SELECT '4. Partially materialized index + postprocessor: haystack is postprocessed on row-scan too.';

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

SELECT '5. Partially materialized index + non-trivial postprocessor: haystack postprocessed on row-scan.';

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

SELECT '6. startsWith / endsWith stay correct across mixed indexed and non-indexed parts.';

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

SELECT '7. Array tokenizer + postprocessor: rewrite path matches index for mixed parts.';

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

SELECT '8. Array column + non-array tokenizer: each element is tokenized before the postprocessor.';
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

SELECT '9. Array column + non-array tokenizer: unmaterialized (row-scan) and indexed parts agree.';

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

SELECT '10. ngrams tokenizer + postprocessor: postprocessed tokens are not re-tokenized.';
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

SELECT '11. sparseGrams + non-injective postprocessor: sparse-gram compaction runs after postprocessing.';
-- sparseGrams('abcdefgh') = ['abc','bcd','cde','cdef','def','efg','fgh']; 'cde' and 'def' are substrings of
-- 'cdef', so sparse-gram compaction drops them. With postprocessor substring(val, 1, 1) the dropped 'def'
-- is the only gram whose first character is 'd', so compacting BEFORE postprocessing loses the token 'd'
-- from the needle. Row 'abcefgh' stores {a,b,c,e,f} (no 'd'): the correct needle for 'abcdefgh' is
-- {a,b,c,d,e,f} and must exclude that row, but a compacted-first needle {a,b,c,e,f} would falsely match it.
-- Compaction now runs after postprocessing, so 'd' is kept and the row is correctly excluded.

CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = sparseGrams(3, 8), postprocessor = substring(val, 1, 1))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, 'abcdefgh'), (2, 'abcefgh');

-- Only row 1 has token 'd', so 'abcdefgh' matches just row 1 (a compacted-first needle wrongly returns 2).
SELECT count() FROM tab WHERE hasAllTokens(val, 'abcdefgh') SETTINGS query_plan_direct_read_from_text_index = 1;  -- 1
SELECT count() FROM tab WHERE hasAllTokens(val, 'abcdefgh') SETTINGS query_plan_direct_read_from_text_index = 0;  -- 1
-- 'abcefgh' has no dropped discriminator and legitimately matches both rows.
SELECT count() FROM tab WHERE hasAllTokens(val, 'abcefgh') SETTINGS query_plan_direct_read_from_text_index = 1;   -- 2
SELECT count() FROM tab WHERE hasAllTokens(val, 'abcefgh') SETTINGS query_plan_direct_read_from_text_index = 0;   -- 2

DROP TABLE tab;

SELECT '12. hasToken / hasAnyTokens / hasAllTokens results are independent of query_plan_direct_read_from_text_index.';
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

SELECT '13. Array tokenizer + postprocessor: empty elements are dropped before postprocessing (row-scan agrees).';
-- The build path tokenizes each element via forEachToken, which skips empty elements before the
-- postprocessor. The row-scan fallback must drop them too, otherwise a postprocessor mapping '' to a
-- non-empty token matches rows whose empty element was never indexed.

CREATE TABLE tab (id UInt64, val Array(String)) ENGINE = MergeTree ORDER BY id;

SYSTEM STOP MERGES tab;

INSERT INTO tab VALUES (1, ['', 'foo']);  -- old part: no index, evaluated by the row-scan fallback

ALTER TABLE tab ADD INDEX idx(val) TYPE text(tokenizer = 'array', postprocessor = if(empty(val), 'EMPTY', val));

INSERT INTO tab VALUES (2, ['', 'foo']);  -- new part: indexed

-- '' is dropped before postprocessing, so 'EMPTY' is never a token: no part matches it.
SELECT count() FROM tab WHERE hasAnyTokens(val, ['EMPTY']);  -- 0
-- 'foo' is a real token in both parts.
SELECT count() FROM tab WHERE hasAnyTokens(val, ['foo']);    -- 2

SYSTEM START MERGES tab;
DROP TABLE tab;

SELECT '14. Array(FixedString) + array tokenizer + postprocessor: fallback normalizes elements to String.';
-- The build path tokenizes to String tokens, so the postprocessor is validated and applied on String. The
-- row-scan fallback must normalize FixedString elements to String too; otherwise a postprocessor like
-- lowerUTF8 (which rejects FixedString) fails on unmaterialized parts even though the index built fine.

CREATE TABLE tab (id UInt64, val Array(FixedString(3))) ENGINE = MergeTree ORDER BY id;

SYSTEM STOP MERGES tab;

INSERT INTO tab VALUES (1, ['FOO', 'BAR']);  -- old part: no index, evaluated by the row-scan fallback

ALTER TABLE tab ADD INDEX idx(val) TYPE text(tokenizer = 'array', postprocessor = lowerUTF8(val));

INSERT INTO tab VALUES (2, ['FOO', 'BAR']);  -- new part: indexed

-- Both rows postprocess to ['foo','bar']; each needle must match both parts (and the fallback must not throw).
SELECT count() FROM tab WHERE hasAnyTokens(val, ['foo']);          -- 2
SELECT count() FROM tab WHERE hasAllTokens(val, ['foo', 'bar']);   -- 2
SELECT count() FROM tab WHERE hasAnyTokens(val, ['xyz']);          -- 0

SYSTEM START MERGES tab;
DROP TABLE tab;

SELECT '15. Lazy apply mode + postprocessor dropping the needle: empty-token query fills zeros, not a short column.';
-- A postprocessor that drops the needle token yields an empty-token hasAnyTokens/hasAllTokens query that
-- matches no rows. Under lazy apply mode the virtual column must still be zero-filled for every row read;
-- otherwise it is shorter than the block (exposed by OR, where granule pruning cannot mask it) and trips
-- the read-result consistency check. posting_list_codec = 'bitpacking' (non-None) is required to engage
-- lazy mode.

CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = if(val = 'stop', '', val), posting_list_codec = 'bitpacking')
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab SELECT number, if(number = 5, 'stop', 'word' || toString(number)) FROM numbers(20);

SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;

-- The 'stop' token is dropped, so hasAnyTokens(['stop']) matches nothing; OR id = 1 keeps the virtual
-- column unmasked. Both apply modes must return 1 (only the id = 1 row).
SELECT count() FROM tab WHERE hasAnyTokens(val, ['stop']) OR id = 1 SETTINGS text_index_posting_list_apply_mode = 'lazy';        -- 1
SELECT count() FROM tab WHERE hasAnyTokens(val, ['stop']) OR id = 1 SETTINGS text_index_posting_list_apply_mode = 'materialize'; -- 1
SELECT count() FROM tab WHERE hasAllTokens(val, ['stop']) OR id = 1 SETTINGS text_index_posting_list_apply_mode = 'lazy';        -- 1
SELECT count() FROM tab WHERE hasAllTokens(val, ['stop']) OR id = 1 SETTINGS text_index_posting_list_apply_mode = 'materialize'; -- 1

DROP TABLE tab;

DROP TABLE IF EXISTS tab;
