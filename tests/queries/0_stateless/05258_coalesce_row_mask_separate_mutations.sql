DROP TABLE IF EXISTS test_05258;

CREATE TABLE test_05258
(
    n UInt64,
    project_id UInt32,
    field_hash UInt32,
    run_id UInt128,
    payload UInt8
)
ENGINE = AggregatingMergeTree
ORDER BY (project_id, field_hash, run_id)
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    number_of_free_entries_in_pool_to_execute_mutation = 0,
    number_of_free_entries_in_pool_to_execute_optimize_entire_partition = 0;

INSERT INTO test_05258 VALUES
    (1, 1, 1, 1, 0), (2, 1, 2, 2, 0), (3, 1, 3, 3, 0),
    (4, 2, 4, 1, 0), (5, 2, 5, 2, 0), (6, 2, 6, 3, 0),
    (7, 3, 7, 1, 0);

SYSTEM STOP MERGES test_05258;

ALTER TABLE test_05258 UPDATE _row_exists = 0
WHERE (project_id = 1) AND (run_id IN (SELECT toUInt128(arrayJoin(['1']))))
SETTINGS mutations_sync = 0;
ALTER TABLE test_05258 UPDATE _row_exists = 0
WHERE (project_id = 2) AND (run_id IN (SELECT toUInt128(arrayJoin(['1']))))
SETTINGS mutations_sync = 0;
ALTER TABLE test_05258 UPDATE _row_exists = 0
WHERE (project_id = 1) AND (run_id IN (SELECT toUInt128(arrayJoin(['2']))))
SETTINGS mutations_sync = 0;

ALTER TABLE test_05258 UPDATE payload = 9 WHERE n = 6 SETTINGS mutations_sync = 0;

ALTER TABLE test_05258 UPDATE _row_exists = 0
WHERE (project_id = 1) AND (run_id IN (SELECT toUInt128(arrayJoin(['3']))))
SETTINGS mutations_sync = 0;
ALTER TABLE test_05258 UPDATE _row_exists = 0
WHERE (project_id = 2) AND (run_id IN (SELECT toUInt128(arrayJoin(['2']))))
SETTINGS mutations_sync = 0;

SYSTEM START MERGES test_05258;
ALTER TABLE test_05258 UPDATE payload = payload WHERE n = 7 SETTINGS mutations_sync = 2;

SELECT n, project_id, run_id, payload FROM test_05258 ORDER BY n;
SELECT count(), countIf(is_done) FROM system.mutations
WHERE database = currentDatabase() AND table = 'test_05258';

DROP TABLE test_05258;
