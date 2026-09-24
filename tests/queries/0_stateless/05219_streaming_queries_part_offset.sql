SET enable_streaming_queries = 1,
    enable_parallel_replicas = 0,
    automatic_parallel_replicas_mode = 0,
    max_threads = 4,
    max_execution_time = 20,
    use_query_condition_cache = 0;

CREATE TABLE repro_offsets
(
    x UInt64,
    payload UInt64,
    PROJECTION commit_order INDEX *, _part_offset TYPE commit_order
        WITH SETTINGS (index_granularity = 1)
)
ENGINE = MergeTree
ORDER BY x
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    allow_commit_order_projection = 1,
    part_minmax_index_columns = 'with_block_number_offset',
    add_minmax_index_for_block_number_column = 1,
    add_minmax_index_for_block_offset_column = 1,
    index_granularity = 8,
    merge_selector_algorithm = 'Manual';

INSERT INTO repro_offsets
SELECT 2 * number, number FROM numbers(8);

INSERT INTO repro_offsets
SELECT 2 * number + 1, number + 8 FROM numbers(8);

OPTIMIZE TABLE repro_offsets FINAL;

SELECT 'base_table';
SELECT payload
FROM repro_offsets
WHERE _part_offset = 1
SETTINGS optimize_use_projections = 0;

SELECT 'stream';
-- `prefer_optimize_projection` takes the commit-order projection regardless of its estimated
-- cost, and `_part_offset` then numbers the projection's rows rather than the parent part's,
-- which is the very mapping this test checks. The base-table query above is pinned for the
-- same reason, with `optimize_use_projections = 0`.
SELECT payload
FROM repro_offsets STREAM BOUNDED
WHERE _part_offset = 1
SETTINGS prefer_optimize_projection = 0;
