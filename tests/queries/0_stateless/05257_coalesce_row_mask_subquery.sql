DROP TABLE IF EXISTS test_05257;

CREATE TABLE test_05257
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
    min_bytes_for_full_part_storage = 0;

INSERT INTO test_05257 VALUES
    (1, 1, 1, 1, 0), (2, 1, 2, 2, 0), (3, 1, 3, 3, 0),
    (4, 2, 4, 1, 0), (5, 1, 5, 4, 0), (6, 1, 6, 5, 0),
    (7, 2, 7, 5, 0), (8, 3, 8, 1, 0);

ALTER TABLE test_05257
    UPDATE _row_exists = 0 WHERE (project_id = 1) AND (run_id IN (SELECT toUInt128(arrayJoin(['1'])))),
    UPDATE _row_exists = 0 WHERE (project_id = 1) AND (run_id IN (SELECT toUInt128(arrayJoin(['2'])))),
    UPDATE _row_exists = 0 WHERE (project_id = 1) AND (run_id IN (SELECT toUInt128(arrayJoin(['2'])))),
    UPDATE _row_exists = 0 WHERE (project_id = 1) AND (run_id IN (SELECT toUInt128(arrayJoin(['99'])))),
    UPDATE payload = 1 WHERE project_id = 1 AND run_id = 3,
    UPDATE _row_exists = 0 WHERE (project_id = 1) AND (run_id IN (SELECT toUInt128(arrayJoin(['4'])))),
    UPDATE _row_exists = 0 WHERE (project_id = 1) AND (run_id IN (SELECT toUInt128(arrayJoin(['5'])))),
    UPDATE _row_exists = 0 WHERE (project_id = 2) AND (run_id IN (SELECT toUInt128(arrayJoin(['1']))))
SETTINGS mutations_sync = 2;

SELECT n, project_id, run_id, payload FROM test_05257 ORDER BY n;

DROP TABLE test_05257;
