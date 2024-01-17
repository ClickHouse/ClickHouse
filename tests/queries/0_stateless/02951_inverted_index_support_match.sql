SET allow_experimental_inverted_index = true;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    str String,
    INDEX inv_idx(str) TYPE inverted(0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab VALUES (1, 'Hello ClickHouse'), (2, 'Hello World'), (3, 'Good Weather'), (4, 'Say Hello'), (5, 'OLAP Database'), (6, 'World Champion');

SELECT * FROM tab WHERE match(str, 'Hello (ClickHouse|World)') ORDER BY id;

-- Read 2/6 granules
-- Required string: 'Hello '
-- Alternatives: 'Hello ClickHouse', 'Hello World'

SELECT *
FROM
(
    EXPLAIN PLAN indexes=1
    SELECT * FROM tab WHERE match(str, 'Hello (ClickHouse|World)') ORDER BY id
)
WHERE
    explain LIKE '%Granules: %'
SETTINGS
    allow_experimental_analyzer = 0;

SELECT *
FROM
(
    EXPLAIN PLAN indexes=1
    SELECT * FROM tab WHERE match(str, 'Hello (ClickHouse|World)') ORDER BY id
)
WHERE
    explain LIKE '%Granules: %'
SETTINGS
    allow_experimental_analyzer = 1;

SELECT '---';

SELECT * FROM tab WHERE match(str, '.*(ClickHouse|World)') ORDER BY id;

-- Read 3/6 granules
-- Required string: -
-- Alternatives: 'ClickHouse', 'World'

SELECT *
FROM
(
    EXPLAIN PLAN indexes = 1
    SELECT * FROM tab WHERE match(str, '.*(ClickHouse|World)') ORDER BY id
)
WHERE
    explain LIKE '%Granules: %'
SETTINGS
    allow_experimental_analyzer = 0;

SELECT *
FROM
(
    EXPLAIN PLAN indexes = 1
    SELECT * FROM tab WHERE match(str, '.*(ClickHouse|World)') ORDER BY id
)
WHERE
    explain LIKE '%Granules: %'
SETTINGS
    allow_experimental_analyzer = 1;

SELECT '---';

SELECT * FROM tab WHERE match(str, 'OLAP.*') ORDER BY id;

-- Read 1/6 granules
-- Required string: 'OLAP'
-- Alternatives: -

SELECT *
FROM
(
    EXPLAIN PLAN indexes = 1
    SELECT * FROM tab WHERE match(str, 'OLAP (.*?)*') ORDER BY id
)
WHERE
    explain LIKE '%Granules: %'
SETTINGS
    allow_experimental_analyzer = 0;

SELECT *
FROM
(
    EXPLAIN PLAN indexes = 1
    SELECT * FROM tab WHERE match(str, 'OLAP (.*?)*') ORDER BY id
)
WHERE
    explain LIKE '%Granules: %'
SETTINGS
    allow_experimental_analyzer = 1;

DROP TABLE tab;
