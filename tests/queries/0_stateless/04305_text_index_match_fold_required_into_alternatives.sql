-- Tags: no-parallel-replicas

-- Tests that match() folds the regexp's required substring into every alternative.
--
-- For a pattern like '(abc|xyz)q* one delta two' the alternation branches 'abc' / 'xyz' are
-- single words whose tokens are dropped at the substring boundaries, so on their own they
-- impose no token requirement and would disable index pruning entirely. The literal run
-- ' one delta two' that must appear regardless of the chosen branch contributes the interior
-- tokens 'one' and 'delta'. Folding them into every alternative restores pruning down to the
-- matching granules without affecting the result.

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    s String,
    INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab VALUES
    (1, 'abc one delta two'),
    (2, 'xyz one delta two'),
    (3, 'abc one two three'),
    (4, 'hello world foobar'),
    (5, 'qqq delta rrr');

SELECT '-- match returns the same rows with and without the index';
SELECT id FROM tab WHERE match(s, '(abc|xyz)q* one delta two') ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT id FROM tab WHERE match(s, '(abc|xyz)q* one delta two') ORDER BY id SETTINGS use_skip_indexes = 1;

SELECT '-- the required substring is folded into every alternative (tokens: delta, one)';
SELECT trim(explain)
FROM
(
    EXPLAIN PLAN indexes = 1
    SELECT id FROM tab WHERE match(s, '(abc|xyz)q* one delta two')
)
WHERE explain LIKE '%mode:%';

SELECT '-- so the text index prunes to the 2 matching granules (out of 5)';
SELECT trim(explain)
FROM
(
    EXPLAIN PLAN indexes = 1
    SELECT id FROM tab WHERE match(s, '(abc|xyz)q* one delta two')
)
WHERE explain LIKE '%Granules: %';

DROP TABLE tab;
