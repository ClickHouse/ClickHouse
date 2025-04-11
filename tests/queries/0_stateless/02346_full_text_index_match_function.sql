-- Tests that match() utilizes the full_text index

SET allow_experimental_full_text_index = true;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    k UInt32,
    v String,
    INDEX inv_idx(v) TYPE full_text(0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY k
SETTINGS index_granularity = 1;

INSERT INTO tab VALUES (1, 'Well, Hello ClickHouse !'), (2, 'Well, Hello World !'), (3, 'Good Weather !'), (4, 'Say Hello !'), (5, 'Its An OLAP Database'), (6, 'True World Champion');

SELECT * FROM tab WHERE match(v, ' Hello (ClickHouse|World) ') ORDER BY k;

-- Read 2/6 granules
-- Required string: ' Hello '
-- Alternatives: ' Hello ClickHouse ', ' Hello World '

SELECT *
FROM
(
    EXPLAIN PLAN indexes=1
    SELECT * FROM tab WHERE match(v, ' Hello (ClickHouse|World) ') ORDER BY k
)
WHERE
    explain LIKE '%Granules: %'
SETTINGS
    enable_analyzer = 0;

SELECT *
FROM
(
    EXPLAIN PLAN indexes=1
    SELECT * FROM tab WHERE match(v, ' Hello (ClickHouse|World) ') ORDER BY k
)
WHERE
    explain LIKE '%Granules: %'
SETTINGS
    enable_analyzer = 1;

SELECT '---';

SELECT * FROM tab WHERE match(v, '.* (ClickHouse|World) ') ORDER BY k;

-- Read 3/6 granules
-- Required string: -
-- Alternatives: ' ClickHouse ', ' World '

SELECT *
FROM
(
    EXPLAIN PLAN indexes = 1
    SELECT * FROM tab WHERE match(v, '.* (ClickHouse|World) ') ORDER BY k
)
WHERE
    explain LIKE '%Granules: %'
SETTINGS
    enable_analyzer = 0;

SELECT *
FROM
(
    EXPLAIN PLAN indexes = 1
    SELECT * FROM tab WHERE match(v, '.* (ClickHouse|World) ') ORDER BY k
)
WHERE
    explain LIKE '%Granules: %'
SETTINGS
    enable_analyzer = 1;

SELECT '---';

SELECT * FROM tab WHERE match(v, ' OLAP .*') ORDER BY k;

-- Read 1/6 granules
-- Required string: ' OLAP '
-- Alternatives: -

SELECT *
FROM
(
    EXPLAIN PLAN indexes = 1
    SELECT * FROM tab WHERE match(v, ' OLAP (.*?)*') ORDER BY k
)
WHERE
    explain LIKE '%Granules: %'
SETTINGS
    enable_analyzer = 0;

SELECT *
FROM
(
    EXPLAIN PLAN indexes = 1
    SELECT * FROM tab WHERE match(v, ' OLAP (.*?)*') ORDER BY k
)
WHERE
    explain LIKE '%Granules: %'
SETTINGS
    enable_analyzer = 1;

DROP TABLE tab;
