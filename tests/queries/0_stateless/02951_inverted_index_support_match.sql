SET allow_experimental_analyzer = 1;
SET allow_experimental_inverted_index = true;
DROP TABLE IF EXISTS inverted_tab;

CREATE TABLE inverted_tab
(
    id UInt32,
    str String,
    INDEX inv_idx(str) TYPE inverted(0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO inverted_tab VALUES (1, 'Hello ClickHouse'), (2, 'Hello World'), (3, 'Good Weather'), (4, 'Say Hello'), (5, 'OLAP Database'), (6, 'World Champion');

SELECT * FROM inverted_tab WHERE match(str, 'Hello (ClickHouse|World)') ORDER BY id;

-- Skip 2/6 granules
-- Required string: 'Hello '
-- Alternatives: 'Hello ClickHouse', 'Hello World'

SELECT *
FROM
(
    EXPLAIN PLAN indexes=1
    SELECT * FROM inverted_tab WHERE match(str, 'Hello (ClickHouse|World)') ORDER BY id
)
WHERE
  explain LIKE '%Granules: %';

SELECT '---';

SELECT * FROM inverted_tab WHERE match(str, '.*(ClickHouse|World)') ORDER BY id;

-- Skip 3/6 granules
-- Required string: -
-- Alternatives: 'ClickHouse', 'World'

SELECT *
FROM
(
    EXPLAIN PLAN indexes = 1
    SELECT * FROM inverted_tab WHERE match(str, '.*(ClickHouse|World)') ORDER BY id
)
WHERE
  explain LIKE '%Granules: %';

SELECT '---';

SELECT * FROM inverted_tab WHERE match(str, 'OLAP.*') ORDER BY id;

-- Skip 5/6 granules
-- Required string: 'OLAP'
-- Alternatives: -

SELECT *
FROM
(
    EXPLAIN PLAN indexes = 1
    SELECT * FROM inverted_tab WHERE match(str, 'OLAP (.*?)*') ORDER BY id
)
WHERE
  explain LIKE '%Granules: %';


