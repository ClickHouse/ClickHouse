-- More tests for use_skip_index_if_final_exact_mode optimization

SET use_skip_indexes = 1;
SET use_skip_indexes_if_final = 1;
SET use_skip_indexes_if_final_exact_mode = 1;

DROP TABLE IF EXISTS tab1;

CREATE TABLE tab1 (id Int32, v Int32, INDEX secondaryidx v TYPE minmax) ENGINE=ReplacingMergeTree ORDER BY id SETTINGS index_granularity=2;

SYSTEM STOP MERGES tab1;

INSERT INTO tab1 SELECT number, number FROM numbers(10);
INSERT INTO tab1 SELECT number, 100 + number FROM numbers(10); -- 'v' column has changed

-- Should correctly read 1st granule in 2nd part and return no rows
SELECT id FROM tab1 FINAL WHERE v = 0;

-- Should correctly read last granule in 2nd part and return no rows
SELECT id FROM tab1 FINAL WHERE v = 9;

-- All these queries will return 0 rows by correctly reading extra granule from 2nd part
SELECT id FROM tab1 FINAL WHERE v = 1;
SELECT id FROM tab1 FINAL WHERE v = 2;
SELECT id FROM tab1 FINAL WHERE v = 3;
SELECT id FROM tab1 FINAL WHERE v = 4;
SELECT id FROM tab1 FINAL WHERE v = 5;
SELECT id FROM tab1 FINAL WHERE v = 6;
SELECT id FROM tab1 FINAL WHERE v = 7;
SELECT id FROM tab1 FINAL WHERE v = 8;

INSERT INTO tab1 VALUES (0, 8888), (9, 9999);

-- Should correctly read 1st granule in 2nd part & 1st granule in 3rd part
SELECT id FROM tab1 FINAL WHERE v = 0;

-- Should correctly read last granule in 2nd part & 1st granule in 3rd part
SELECT id FROM tab1 FINAL WHERE v = 9;

-- Rows with id = 0 and id = 9 should be printed
SELECT id FROM tab1 FINAL WHERE v = 8888;
SELECT id FROM tab1 FINAL WHERE v = 9999;


-- Test for repeated PK range. Rows will have PK like this -
--    (1,1,1,<v>), (1,1,2,<v>), (1,1,3,<v>), ...
-- Test for PR https://github.com/ClickHouse/ClickHouse/pull/82667

DROP TABLE IF EXISTS tab2;
CREATE TABLE tab2 (id1 Int32, id2 Int32, id3 Int32, v Int32, INDEX secondaryidx v TYPE minmax) ENGINE=ReplacingMergeTree ORDER BY (id1, id2, id3) SETTINGS index_granularity=64;

SYSTEM STOP MERGES tab2;

INSERT INTO tab2 SELECT (number % 2500), (number % 500), number, number from numbers(10000);
INSERT INTO tab2 SELECT (number % 2500), (number % 500), number, 100000 + number from numbers(10000); -- 'v' column has changed

-- No rows should be selected by below queries as 'v' does not have value < 10000 due to updates in 2nd part
SELECT id1, id2, id3 FROM tab2 FINAL WHERE v = rand() % 10000;
SELECT id1, id2, id3 FROM tab2 FINAL WHERE v = rand() % 10000;
SELECT id1, id2, id3 FROM tab2 FINAL WHERE v = rand() % 10000;
SELECT id1, id2, id3 FROM tab2 FINAL WHERE v = rand() % 10000;
SELECT id1, id2, id3 FROM tab2 FINAL WHERE v = rand() % 10000;
SELECT id1, id2, id3 FROM tab2 FINAL WHERE v = rand() % 10000;
SELECT id1, id2, id3 FROM tab2 FINAL WHERE v = rand() % 10000;
SELECT id1, id2, id3 FROM tab2 FINAL WHERE v = rand() % 10000;
SELECT id1, id2, id3 FROM tab2 FINAL WHERE v = rand() % 10000;
SELECT id1, id2, id3 FROM tab2 FINAL WHERE v = rand() % 10000;

-- Tests with single range parts (https://github.com/ClickHouse/ClickHouse/issues/82792)
DROP TABLE IF EXISTS tab3;

CREATE TABLE tab3(
    key Int,
    value Int,
    INDEX idx value TYPE minmax GRANULARITY 1
)
Engine=ReplacingMergeTree()
ORDER BY key
PARTITION BY key;

SYSTEM STOP MERGES tab3;

INSERT INTO tab3 SELECT number, number FROM numbers(10); -- 10 parts

SELECT key, value FROM tab3 FINAL WHERE value = 1 SETTINGS max_rows_to_read = 1; -- 1,1

INSERT INTO tab3 VALUES (0, 100), (1, 101), (2, 102), (3, 103), (4, 104), (5, 105), (6, 106), (7, 107), (8, 108), (9, 109); -- 10 more parts

-- Next statements return 0 rows. Read 1 range each from 2 parts
SELECT key, value FROM tab3 FINAL WHERE value = 0 SETTINGS max_rows_to_read = 2;
SELECT key, value FROM tab3 FINAL WHERE value = 1 SETTINGS max_rows_to_read = 2;
SELECT key, value FROM tab3 FINAL WHERE value = 2 SETTINGS max_rows_to_read = 2;
SELECT key, value FROM tab3 FINAL WHERE value = 3 SETTINGS max_rows_to_read = 2;
SELECT key, value FROM tab3 FINAL WHERE value = 4 SETTINGS max_rows_to_read = 2;
SELECT key, value FROM tab3 FINAL WHERE value = 5 SETTINGS max_rows_to_read = 2;
SELECT key, value FROM tab3 FINAL WHERE value = 6 SETTINGS max_rows_to_read = 2;
SELECT key, value FROM tab3 FINAL WHERE value = 7 SETTINGS max_rows_to_read = 2;
SELECT key, value FROM tab3 FINAL WHERE value = 8 SETTINGS max_rows_to_read = 2;
SELECT key, value FROM tab3 FINAL WHERE value = 9 SETTINGS max_rows_to_read = 2;

DROP TABLE tab1;
DROP TABLE tab2;
DROP TABLE tab3;
