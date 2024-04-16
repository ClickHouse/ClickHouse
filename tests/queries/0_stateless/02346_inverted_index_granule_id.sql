-- Test that inverted index uses row IDs or granule IDs in posting list, indirectly

SET allow_experimental_inverted_index = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    str String,
    INDEX inv_idx(str) TYPE inverted(3) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 16;

-- Use row IDs
ALTER TABLE tab MODIFY SETTING inverted_index_map_to_granule_id = 0;

INSERT INTO tab VALUES (1, 'Hello ClickHouse'), (2, 'Hello World'), (3, 'Good Weather');

SELECT str FROM tab WHERE str LIKE 'Hello%Weather';
SELECT *
FROM
(
    EXPLAIN PLAN indexes = 1
    SELECT * FROM tab WHERE str LIKE 'Hello%Weather' ORDER BY id
)
WHERE
    explain LIKE '%Granules: %';

-- Use granule IDs
ALTER TABLE tab MODIFY SETTING inverted_index_map_to_granule_id = 1;

INSERT INTO tab VALUES (4, 'Say Hello'), (5, 'OLAP Database'), (6, 'World Champion');

SELECT str FROM tab WHERE str LIKE 'Say%Hello';
SELECT *
FROM
(
    EXPLAIN PLAN indexes = 1
    SELECT * FROM tab WHERE str LIKE 'Say%Hello' ORDER BY id
)
WHERE
    explain LIKE '%Granules: %';
SELECT str FROM tab WHERE str LIKE 'Hello%Champion';
SELECT *
FROM
(
    EXPLAIN PLAN indexes = 1
    SELECT * FROM tab WHERE str LIKE 'Hello%Champion' ORDER BY id
)
WHERE
    explain LIKE '%Granules: %';
