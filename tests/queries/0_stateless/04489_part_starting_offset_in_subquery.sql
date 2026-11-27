-- A total-offset predicate written as IN must still prune marks.
-- `max_rows_to_read` counts rows per reading node, so parallel replicas must stay off for the
-- bound to measure this server's pruning.

DROP TABLE IF EXISTS test_offset_qf;

CREATE TABLE test_offset_qf (user_id UInt32, value UInt32)
ENGINE = MergeTree ORDER BY user_id
SETTINGS index_granularity = 1, max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO test_offset_qf VALUES (1, 100);
INSERT INTO test_offset_qf VALUES (2, 200);
INSERT INTO test_offset_qf VALUES (3, 300);

SELECT value
FROM test_offset_qf
WHERE (_part_starting_offset + _part_offset) IN (
    SELECT _part_starting_offset + _part_offset
    FROM test_offset_qf
    WHERE user_id = 2
)
ORDER BY value
SETTINGS parallel_replicas_local_plan = 0, max_rows_to_read = 2, enable_parallel_replicas = 0;

SELECT user_id, value
FROM test_offset_qf
WHERE (_part_starting_offset + _part_offset) IN (0, 2)
ORDER BY user_id
SETTINGS parallel_replicas_local_plan = 0, max_rows_to_read = 2, enable_parallel_replicas = 0;

DROP TABLE test_offset_qf;
