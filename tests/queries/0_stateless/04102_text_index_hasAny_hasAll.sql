-- Tests that the text index is used for `hasAny` and `hasAll` predicates.

SET enable_analyzer = 1;
SET query_plan_remove_unused_columns = 1;
SET query_plan_optimize_prewhere = 1;
SET optimize_move_to_prewhere = 1;

DROP TABLE IF EXISTS tab_array;
DROP TABLE IF EXISTS tab_split;

-- ---------------------------------------------------------------------------
SELECT '-- array tokenizer (Exact direct read)';

CREATE TABLE tab_array
(
    id UInt32,
    arr Array(String),
    INDEX arr_idx(arr) TYPE text(tokenizer = 'array')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab_array VALUES
    (1, ['foo', 'bar']),
    (2, ['bar', 'baz']),
    (3, ['foo', 'baz']),
    (4, ['qux']);

SELECT groupArray(id) FROM tab_array WHERE hasAny(arr, ['foo']) ORDER BY ALL;
SELECT groupArray(id) FROM tab_array WHERE hasAny(arr, ['foo', 'qux']) ORDER BY ALL;
SELECT groupArray(id) FROM tab_array WHERE hasAny(arr, ['missing']) ORDER BY ALL;

SELECT groupArray(id) FROM tab_array WHERE hasAll(arr, ['foo']) ORDER BY ALL;
SELECT groupArray(id) FROM tab_array WHERE hasAll(arr, ['foo', 'bar']) ORDER BY ALL;
SELECT groupArray(id) FROM tab_array WHERE hasAll(arr, ['foo', 'missing']) ORDER BY ALL;

-- Empty needle: vacuous truth for hasAll, vacuous falsity for hasAny.
SELECT count() FROM tab_array WHERE hasAny(arr, []);
SELECT count() FROM tab_array WHERE hasAll(arr, []);

SELECT '-- array tokenizer: index is used';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1, actions = 1 SELECT count() FROM tab_array WHERE hasAny(arr, ['foo', 'baz'])
) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%' OR explain LIKE '%INPUT%\_\_text_index%';

SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1, actions = 1 SELECT count() FROM tab_array WHERE hasAll(arr, ['foo', 'bar'])
) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%' OR explain LIKE '%INPUT%\_\_text_index%';

DROP TABLE tab_array;

-- ---------------------------------------------------------------------------
SELECT '-- splitByNonAlpha tokenizer (per-element queries)';

CREATE TABLE tab_split
(
    id UInt32,
    arr Array(String),
    INDEX arr_idx(arr) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab_split VALUES
    (1, ['hello world', 'foo bar']),
    (2, ['hello there', 'foo bar']),
    (3, ['foo bar', 'baz qux']),
    (4, ['other text']);

-- hasAny: match if any needle element is present.
SELECT groupArray(id) FROM tab_split WHERE hasAny(arr, ['hello world']) ORDER BY ALL;
SELECT groupArray(id) FROM tab_split WHERE hasAny(arr, ['hello world', 'baz qux']) ORDER BY ALL;
SELECT groupArray(id) FROM tab_split WHERE hasAny(arr, ['nothing here']) ORDER BY ALL;

-- hasAll: every needle element must be present.
SELECT groupArray(id) FROM tab_split WHERE hasAll(arr, ['foo bar']) ORDER BY ALL;
SELECT groupArray(id) FROM tab_split WHERE hasAll(arr, ['hello world', 'foo bar']) ORDER BY ALL;
SELECT groupArray(id) FROM tab_split WHERE hasAll(arr, ['hello world', 'missing word']) ORDER BY ALL;

-- Empty needle: same edge cases as for the array tokenizer.
SELECT count() FROM tab_split WHERE hasAny(arr, []);
SELECT count() FROM tab_split WHERE hasAll(arr, []);

SELECT '-- splitByNonAlpha: index is still used';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1, actions = 1 SELECT count() FROM tab_split WHERE hasAny(arr, ['hello world', 'baz qux'])
) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%' OR explain LIKE '%INPUT%\_\_text_index%';

SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1, actions = 1 SELECT count() FROM tab_split WHERE hasAll(arr, ['hello world', 'foo bar'])
) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%' OR explain LIKE '%INPUT%\_\_text_index%';

DROP TABLE tab_split;
