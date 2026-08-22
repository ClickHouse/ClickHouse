DROP TABLE IF EXISTS test_apply_deleted_mask;

CREATE TABLE test_apply_deleted_mask(id Int64, value String) ENGINE = MergeTree ORDER BY id;

INSERT INTO test_apply_deleted_mask SELECT number, number::String FROM numbers(5);

DELETE FROM test_apply_deleted_mask WHERE id % 2 = 0;

SELECT 'Normal SELECT does not see deleted rows';
SELECT *, _row_exists FROM test_apply_deleted_mask;

SELECT 'With the setting disabled the deleted rows are visible';
SELECT *, _row_exists FROM test_apply_deleted_mask SETTINGS apply_deleted_mask = 0;

SELECT 'With the setting disabled the deleted rows are visible but still can be filterd out';
SELECT * FROM test_apply_deleted_mask WHERE _row_exists SETTINGS apply_deleted_mask = 0;

INSERT INTO test_apply_deleted_mask SELECT number, number::String FROM numbers(5, 1);

OPTIMIZE TABLE test_apply_deleted_mask FINAL SETTINGS mutations_sync=2;

SELECT 'Read the data after OPTIMIZE, all deleted rwos should be physically removed now';
SELECT *, _row_exists FROM test_apply_deleted_mask SETTINGS apply_deleted_mask = 0;

DROP TABLE test_apply_deleted_mask;