-- Tests the postprocessor argument in text indexes: results are identical whether a part is indexed
-- (new/materialized parts use the index) or falls back to a row scan (old/unmaterialized/partially
-- materialized parts), including after ALTER ... MATERIALIZE INDEX.

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

SELECT '2. Partially materialized index.';

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

SELECT '3. Partially materialized index + postprocessor: haystack is postprocessed on row-scan too.';

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

SELECT '4. Partially materialized index + non-trivial postprocessor: haystack postprocessed on row-scan.';

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

SELECT '5. startsWith / endsWith stay correct across mixed indexed and non-indexed parts.';

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

SELECT '6. Array tokenizer + postprocessor: rewrite path matches index for mixed parts.';

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

SELECT '7. Array column + non-array tokenizer: unmaterialized (row-scan) and indexed parts agree.';

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
