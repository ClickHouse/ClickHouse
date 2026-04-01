-- Tags: no-parallel-replicas

SET parallel_replicas_local_plan = 1;
SET use_query_condition_cache = 0;
SET use_skip_indexes_on_data_read = 1;
SET use_skip_indexes = 1;
SET optimize_projection_skip_index_ratio = 0.5;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id1 UInt32,
    id2 String,
    v1 UInt32,
    INDEX v1_index v1 TYPE minmax,
    PROJECTION proj1 (SELECT * ORDER BY id2)
)
ENGINE = MergeTree
ORDER BY id1
SETTINGS
    index_granularity = 64, index_granularity_bytes = 0,
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    max_bytes_to_merge_at_max_space_in_pool = 1,
    use_const_adaptive_granularity = 1;

INSERT INTO tab SELECT number + 1, concat('k', toString(number + 1)), number + 1 FROM numbers(10000);

SELECT COUNT(*) FROM tab WHERE v1 <= 500;

SELECT COUNT(*) FROM tab WHERE id2 > 'k3';

-- projection should not be used
SELECT COUNT(*) FROM tab WHERE id2 > 'k3' AND v1 <= 500 SETTINGS max_rows_to_read = 640; -- 10 granules

SELECT COUNT(*) FROM tab WHERE id2 >= 'k7';

-- projection will be used
SELECT COUNT(*) FROM tab WHERE id2 >= 'k7' AND v1 <= 500 SETTINGS max_rows_to_read = 4000;

DROP TABLE tab;
