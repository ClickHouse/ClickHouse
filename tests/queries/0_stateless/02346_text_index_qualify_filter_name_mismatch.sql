-- Regression test for a bug where text index preprocessing modified the filter DAG
-- (recreating the AND function node with a different result_name) but
-- processAndOptimizeTextIndexDAG returned nullptr because no virtual columns were added,
-- causing the FilterStep's filter_column_name to become inconsistent with the DAG.
-- This triggered a LOGICAL_ERROR in applyOrder.

SET enable_analyzer = 1;

SELECT 'Case 1: Map column with text index on mapValues (original reproduction case)';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt64,
    val Map(String, String),
    INDEX idx mapValues(val) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES (1, {'a': 'foo'}), (2, {'b': 'bar'});

-- The combination of PREWHERE + WHERE + QUALIFY with a text index function triggers the bug.
-- QUALIFY merges into WHERE as AND, then text index preprocessing rewrites hasAnyTokens
-- but returns nullptr (no virtual columns added), leaving filter_column_name stale.
SELECT DISTINCT id
FROM tab
PREWHERE hasAnyTokens(mapValues(val), 'foo')
WHERE hasAnyTokens(mapValues(val), 'foo')
QUALIFY id
ORDER BY id;

DROP TABLE tab;

SELECT 'Case 2: Simple String column with text index';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx val TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES (1, 'hello world'), (2, 'goodbye world'), (3, 'hello foo');

SELECT DISTINCT id
FROM tab
PREWHERE hasAnyTokens(val, 'hello')
WHERE hasAnyTokens(val, 'hello')
QUALIFY id
ORDER BY id;

-- Also test hasAllTokens on a simple String column
SELECT DISTINCT id
FROM tab
PREWHERE hasAllTokens(val, 'hello world')
WHERE hasAllTokens(val, 'hello world')
QUALIFY id
ORDER BY id;

DROP TABLE tab;

SELECT 'Case 3: Text index with preprocessor (e.g. lower) triggers the same code path';
-- The preprocessor modifies the DAG additionally by wrapping the column with the preprocessor function

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx val TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(val))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES (1, 'Hello World'), (2, 'Goodbye World'), (3, 'HELLO FOO');

SELECT DISTINCT id
FROM tab
PREWHERE hasAnyTokens(val, 'hello')
WHERE hasAnyTokens(val, 'hello')
QUALIFY id
ORDER BY id;

SELECT DISTINCT id
FROM tab
PREWHERE hasAllTokens(val, 'hello world')
WHERE hasAllTokens(val, 'hello world')
QUALIFY id
ORDER BY id;

DROP TABLE tab;
