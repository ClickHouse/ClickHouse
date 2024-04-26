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
SETTINGS index_granularity = 4;

-- Use row IDs
ALTER TABLE tab MODIFY SETTING inverted_index_row_id_divisor = 1;

INSERT INTO tab VALUES (1, 'bubble01'), (2, 'bubble02'), (3, 'bubble03'), (4, 'bubble04');
INSERT INTO tab VALUES (5, 'bubble05'), (6, 'bubble06'), (7, 'bubble07'), (8, 'bubble08');

SELECT 'divisor = 1';
SELECT str FROM tab WHERE str LIKE 'bubble01%bubble02';
SELECT *
FROM
(
    EXPLAIN PLAN indexes = 1
    SELECT * FROM tab WHERE str LIKE 'bubble01%bubble02' ORDER BY id
)
WHERE
    explain LIKE '%Granules: %';

-- Use shared row IDs
SELECT 'divisor = 2';
ALTER TABLE tab MODIFY SETTING inverted_index_row_id_divisor = 2; -- row id increases for each 2 rows.

TRUNCATE TABLE tab SYNC;

INSERT INTO tab VALUES (1, 'bubble01'), (2, 'bubble02'), (3, 'bubble03'), (4, 'bubble04');
INSERT INTO tab VALUES (5, 'bubble05'), (6, 'bubble06'), (7, 'bubble07'), (8, 'bubble08');

-- row ID counts from 1. so 'bubble02' and 'bubble03' share a same id.
SELECT str FROM tab WHERE str LIKE 'bubble02%bubble03';
SELECT *
FROM
(
    EXPLAIN PLAN indexes = 1
    SELECT * FROM tab WHERE str LIKE 'bubble02%bubble03' ORDER BY id
)
WHERE
    explain LIKE '%Granules: %';
SELECT str FROM tab WHERE str LIKE 'bubble05%bubble06';
SELECT *
FROM
(
    EXPLAIN PLAN indexes = 1
    SELECT * FROM tab WHERE str LIKE 'bubble05%bubble06' ORDER BY id
)
WHERE
    explain LIKE '%Granules: %';
