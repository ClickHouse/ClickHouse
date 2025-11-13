-- Tags: no-parallel-replicas

-- Tests that match() utilizes the text index

SET allow_experimental_full_text_index = true;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    str String,
    INDEX inv_idx(str) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;
INSERT INTO tab VALUES (1, 'Well, Hello ClickHouse !'), (2, 'Well, Hello World !'), (3, 'Good Weather !'), (4, 'Say Hello !'), (5, 'Its An OLAP Database'), (6, 'True World Champion');

SELECT * FROM tab WHERE match(str, ' Hello (ClickHouse|World) ') ORDER BY id;

-- Read 2/6 granules
-- Required string: ' Hello '
-- Alternatives: ' Hello ClickHouse ', ' Hello World '

SELECT trim(explain)
FROM
(
    EXPLAIN PLAN indexes=1
    SELECT * FROM tab WHERE match(str, ' Hello (ClickHouse|World) ') ORDER BY id
)
WHERE
    explain LIKE '%Granules: %'
SETTINGS
    enable_analyzer = 0;

SELECT trim(explain)
FROM
(
    EXPLAIN PLAN indexes=1
    SELECT * FROM tab WHERE match(str, ' Hello (ClickHouse|World) ') ORDER BY id
)
WHERE
    explain LIKE '%Granules: %'
SETTINGS
    enable_analyzer = 1;

SELECT '---';

SELECT * FROM tab WHERE match(str, '.* (ClickHouse|World) ') ORDER BY id;

-- Read 3/6 granules
-- Required string: -
-- Alternatives: ' ClickHouse ', ' World '

SELECT trim(explain)
FROM
(
    EXPLAIN PLAN indexes = 1
    SELECT * FROM tab WHERE match(str, '.* (ClickHouse|World) ') ORDER BY id
)
WHERE
    explain LIKE '%Granules: %'
SETTINGS
    enable_analyzer = 0;

SELECT trim(explain)
FROM
(
    EXPLAIN PLAN indexes = 1
    SELECT * FROM tab WHERE match(str, '.* (ClickHouse|World) ') ORDER BY id
)
WHERE
    explain LIKE '%Granules: %'
SETTINGS
    enable_analyzer = 1;

SELECT '---';

SELECT * FROM tab WHERE match(str, ' OLAP .*') ORDER BY id;

-- Read 1/6 granules
-- Required string: ' OLAP '
-- Alternatives: -

SELECT trim(explain)
FROM
(
    EXPLAIN PLAN indexes = 1
    SELECT * FROM tab WHERE match(str, ' OLAP (.*?)*') ORDER BY id
)
WHERE
    explain LIKE '%Granules: %'
SETTINGS
    enable_analyzer = 0;

SELECT trim(explain)
FROM
(
    EXPLAIN PLAN indexes = 1
    SELECT * FROM tab WHERE match(str, ' OLAP (.*?)*') ORDER BY id
)
WHERE
    explain LIKE '%Granules: %'
SETTINGS
    enable_analyzer = 1;

DROP TABLE tab;
