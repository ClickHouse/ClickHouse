-- Regression test for FutureSet identity mismatch in TTLDeleteFilterStep.
-- When vertical merge is used with TTL ... WHERE col IN (SELECT ...),
-- the FutureSet objects must be shared between CreatingSetStep and the
-- TTLDeleteFilterTransform. Previously, transformPipeline() created new
-- transforms with new FutureSet objects that were never filled.

SET optimize_throw_if_noop = 0;

DROP TABLE IF EXISTS 04029_ttl_delete_subquery;

CREATE TABLE 04029_ttl_delete_subquery
(
    a UInt32,
    timestamp DateTime,
    c1 UInt32,
    c2 UInt32,
    c3 UInt32,
    c4 UInt32
)
ENGINE = MergeTree
ORDER BY a
TTL timestamp + INTERVAL 1 SECOND WHERE a IN (SELECT number FROM system.numbers LIMIT 10)
SETTINGS
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    max_bytes_to_merge_at_max_space_in_pool = 0,
    enable_block_number_column = 0,
    enable_block_offset_column = 0,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_optimize_ttl_delete = 1,
    merge_with_ttl_timeout = 0;

-- Insert two parts so OPTIMIZE FINAL triggers a merge.
INSERT INTO 04029_ttl_delete_subquery SELECT number, now() - 5, number, number + 1, number + 2, number + 3 FROM numbers(100);
INSERT INTO 04029_ttl_delete_subquery SELECT number + 100, now() - 5, number, number + 1, number + 2, number + 3 FROM numbers(100);

OPTIMIZE TABLE 04029_ttl_delete_subquery FINAL;

SYSTEM FLUSH LOGS part_log;
SELECT merged_from, merge_algorithm, rows
FROM system.part_log
WHERE database = currentDatabase()
    AND table = '04029_ttl_delete_subquery'
    AND event_type = 'MergeParts'
ORDER BY event_time_microseconds DESC
LIMIT 1;

SELECT count() FROM 04029_ttl_delete_subquery;

DROP TABLE 04029_ttl_delete_subquery;
