SET mutations_sync = 2;

-- Array(Nullable(String)) already works.
DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
    id UInt64,
    a Array(Nullable(String)),
    INDEX idx a TYPE text(tokenizer = 'array')
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, [NULL]);
SELECT 'array_nullable', count() FROM tab;

DROP TABLE tab;

-- The fix: Array(LowCardinality(Nullable(String))) with a NULL element.
CREATE TABLE tab
(
    id UInt64,
    a Array(LowCardinality(Nullable(String))),
    INDEX idx a TYPE text(tokenizer = 'array')
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, [NULL]);
INSERT INTO tab VALUES (2, ['hello', NULL, 'world']);

SELECT 'array_lc_nullable', count() FROM tab;
SELECT 'has_hello', id FROM tab WHERE has(a, 'hello') ORDER BY id;

OPTIMIZE TABLE tab FINAL;
SELECT 'after_optimize', count() FROM tab;

DROP TABLE tab;

-- Nested stores its field as Array(LowCardinality(Nullable(String))) (fiddle case).
CREATE TABLE tab
(
    id UInt64,
    n Nested(a LowCardinality(Nullable(String))),
    INDEX idx n.a TYPE text(tokenizer = 'array')
)
ENGINE = MergeTree ORDER BY id;
INSERT INTO tab VALUES (1, [NULL]);
SELECT 'nested_lc_nullable', count() FROM tab;

DROP TABLE tab;

-- MATERIALIZE INDEX on pre-existing data must also skip NULL elements.
DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
    id UInt64,
    a Array(LowCardinality(Nullable(String)))
)
ENGINE = MergeTree ORDER BY id;
INSERT INTO tab VALUES (1, ['x', NULL, 'y']);

ALTER TABLE tab ADD INDEX idx a TYPE text(tokenizer = 'array');
ALTER TABLE tab MATERIALIZE INDEX idx;

SELECT 'materialize_lc_nullable', id FROM tab WHERE has(a, 'x') ORDER BY id;

DROP TABLE tab;

-- With a postprocessor
CREATE TABLE tab
(
    id UInt64,
    a Array(LowCardinality(Nullable(String))),
    INDEX idx a TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = lower(a))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, [NULL]);
INSERT INTO tab VALUES (2, ['Hello', NULL, 'World']);

SELECT 'pp_array_lc_nullable', count() FROM tab;
SELECT 'pp_has_hello', id FROM tab WHERE hasAnyTokens(a, 'HELLO') ORDER BY id;

OPTIMIZE TABLE tab FINAL;
SELECT 'pp_after_optimize', count() FROM tab;

DROP TABLE tab;

CREATE TABLE tab
(
    id UInt64,
    a Array(LowCardinality(Nullable(String)))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, ['X', NULL, 'Y']);

ALTER TABLE tab ADD INDEX idx a TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = lower(a));
ALTER TABLE tab MATERIALIZE INDEX idx;

SELECT 'pp_materialize_lc_nullable', id FROM tab WHERE hasAnyTokens(a, 'x') ORDER BY id;

DROP TABLE tab;
