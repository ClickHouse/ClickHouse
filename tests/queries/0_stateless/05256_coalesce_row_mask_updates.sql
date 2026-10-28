DROP TABLE IF EXISTS test_05256_wide;
DROP TABLE IF EXISTS test_05256_compact;
DROP TABLE IF EXISTS test_05256_assignment_alias_off;
DROP TABLE IF EXISTS test_05256_assignment_alias_on;

CREATE TABLE test_05256_wide (n UInt64, project_id UInt32, id String, payload UInt8)
ENGINE = MergeTree ORDER BY n
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0;

CREATE TABLE test_05256_compact (n UInt64, project_id UInt32, id String, payload UInt8)
ENGINE = MergeTree ORDER BY n
SETTINGS min_bytes_for_wide_part = 1000000000000, min_rows_for_wide_part = 1000000000000;

INSERT INTO test_05256_wide VALUES
    (0, 1, 'a', 0), (1, 1, 'b', 0), (2, 1, 'c', 0), (3, 2, 'a', 0),
    (4, 2, 'd', 0), (5, 1, 'd', 0), (6, 1, 'e', 0), (7, 1, 'f', 0);
INSERT INTO test_05256_compact SELECT * FROM test_05256_wide;

ALTER TABLE test_05256_wide
    UPDATE _row_exists = 0 WHERE project_id = 1 AND id IN ('a'),
    UPDATE _row_exists = 0 WHERE project_id = 1 AND id IN ('b'),
    UPDATE _row_exists = 0 WHERE project_id = 1 AND id IN ('b'),
    UPDATE _row_exists = 0 WHERE project_id = 1 AND id IN ('missing'),
    UPDATE _row_exists = 0 WHERE project_id = 1 AND id IN (concat('no', 'match')),
    UPDATE payload = 1 WHERE project_id = 1 AND id IN ('c'),
    UPDATE _row_exists = 0 WHERE project_id = 1 AND id IN ('d'),
    UPDATE _row_exists = 0 WHERE project_id = 1 AND id IN ('e'),
    UPDATE _row_exists = 0 WHERE project_id = 2 AND id IN ('a'),
    UPDATE _row_exists = 0 WHERE project_id = 2 AND id IN ('d')
SETTINGS mutations_sync = 2;

ALTER TABLE test_05256_compact
    UPDATE _row_exists = 0 WHERE project_id = 1 AND id IN ('a'),
    UPDATE _row_exists = 0 WHERE project_id = 1 AND id IN ('b'),
    UPDATE _row_exists = 0 WHERE project_id = 1 AND id IN ('b'),
    UPDATE _row_exists = 0 WHERE project_id = 1 AND id IN ('missing'),
    UPDATE _row_exists = 0 WHERE project_id = 1 AND id IN (concat('no', 'match')),
    UPDATE payload = 1 WHERE project_id = 1 AND id IN ('c'),
    UPDATE _row_exists = 0 WHERE project_id = 1 AND id IN ('d'),
    UPDATE _row_exists = 0 WHERE project_id = 1 AND id IN ('e'),
    UPDATE _row_exists = 0 WHERE project_id = 2 AND id IN ('a'),
    UPDATE _row_exists = 0 WHERE project_id = 2 AND id IN ('d')
SETTINGS mutations_sync = 2;

SELECT 'wide', n, project_id, id, payload FROM test_05256_wide ORDER BY n;
SELECT 'compact', n, project_id, id, payload FROM test_05256_compact ORDER BY n;

ALTER TABLE test_05256_wide
    UPDATE _row_exists = 0 WHERE project_id = 1 AND id IN ('a'),
    UPDATE _row_exists = 0 WHERE project_id = 1 AND id IN ('absent')
SETTINGS mutations_sync = 2;
SELECT 'again', n, id FROM test_05256_wide ORDER BY n;

CREATE TABLE test_05256_assignment_alias_off (n UInt32, project_id UInt32, id String)
ENGINE = MergeTree ORDER BY n
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0, enable_row_mask_update_coalescing = 0;

CREATE TABLE test_05256_assignment_alias_on (n UInt32, project_id UInt32, id String)
ENGINE = MergeTree ORDER BY n
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0, enable_row_mask_update_coalescing = 1;

INSERT INTO test_05256_assignment_alias_off VALUES
    (1, 1, 'a'), (2, 1, 'b'), (3, 1, 'c'), (4, 2, 'd');
INSERT INTO test_05256_assignment_alias_on SELECT * FROM test_05256_assignment_alias_off;

ALTER TABLE test_05256_assignment_alias_off
    UPDATE _row_exists = 0 WHERE project_id = 1 AND id IN ('a'),
    UPDATE _row_exists = (0 AS project_id) WHERE project_id = 1 AND id IN ('b')
SETTINGS mutations_sync = 2, prefer_column_name_to_alias = 0;
ALTER TABLE test_05256_assignment_alias_on
    UPDATE _row_exists = 0 WHERE project_id = 1 AND id IN ('a'),
    UPDATE _row_exists = (0 AS project_id) WHERE project_id = 1 AND id IN ('b')
SETTINGS mutations_sync = 2, prefer_column_name_to_alias = 0;

SELECT 'alias_off', n, id FROM test_05256_assignment_alias_off ORDER BY n;
SELECT 'alias_on', n, id FROM test_05256_assignment_alias_on ORDER BY n;

DROP TABLE test_05256_wide;
DROP TABLE test_05256_compact;
DROP TABLE test_05256_assignment_alias_off;
DROP TABLE test_05256_assignment_alias_on;
