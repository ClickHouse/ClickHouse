-- Tags: no-parallel-replicas
-- Test that virtual columns are available when reading from normal projections.

DROP TABLE IF EXISTS test_proj_virtuals;

CREATE TABLE test_proj_virtuals
(
    a UInt32,
    b UInt32,
    PROJECTION p (SELECT a, b ORDER BY b)
)
ENGINE = MergeTree
PARTITION BY a % 3
ORDER BY a
SETTINGS index_granularity = 1, storage_policy = 'default';

INSERT INTO test_proj_virtuals SELECT number, number FROM numbers(100);

SET force_optimize_projection = 1;
SET optimize_use_projections = 1;
SET enable_analyzer = 1;

EXPLAIN indexes = 1, projections = 1
SELECT
    a, b,
    _part,
    _part_index,
    _part_starting_offset,
    _part_uuid,
    _partition_id,
    _part_data_version,
    _disk_name,
    _partition_value
FROM test_proj_virtuals
WHERE b = 10
ORDER BY b;

SELECT
    a, b,
    _part,
    _part_index,
    _part_starting_offset,
    _part_uuid,
    _partition_id,
    _part_data_version,
    _disk_name,
    _partition_value
FROM test_proj_virtuals
WHERE b = 10
ORDER BY b;

DROP TABLE test_proj_virtuals;
