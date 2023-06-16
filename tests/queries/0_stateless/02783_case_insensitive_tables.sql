SET enable_case_insensitive_tables=1;
drop table if exists cell_towers_test;

CREATE TABLE cell_towers_test
(
    id UInt64,
    name String
) ENGINE = MergeTree()
ORDER BY id;

-- Test INSERTs with different case
INSERT INTO cell_towers_test (id, name) VALUES(1, 'Tower A');
INSERT INTO cell_towers_TEST (id, name) VALUES(2, 'Tower B'),
INSERT INTO CELL_TOWERS_TEST (id, name) VALUES(3, 'Tower C');

-- Test SELECTs with different case
SELECT * FROM cell_towers_test ORDER BY id;
SELECT * FROM CELL_TOWERS_TEST ORDER BY id;
SELECT * FROM Cell_Towers_Test ORDER BY id;

-- Test ALTERs with different case
ALTER TABLE CELL_TOWERS_TEST ADD COLUMN location String;
INSERT INTO CELL_TOWERS_TEST (id, name, location) VALUES(3, 'Tower D', 'Location D');

SHOW TABLES;

SELECT * FROM Cell_Towers_Test ORDER BY id;

-- Test subquery with different case
SELECT * FROM cell_towers_test WHERE id IN (SELECT id FROM CELL_TOWERS_TEST WHERE name = 'Tower D') ORDER BY id;

-- Test SHOW CREATE TABLE with different case
SHOW CREATE TABLE Cell_Towers_Test;

-- Rename clause test does not work on Mac due to case-insensitive file system. Commenting out until fixed
-- RENAME TABLE cell_towers_test TO CELL_towers_test;

drop table cell_towers_TEsT;
