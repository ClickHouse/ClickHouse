-- Tags: no-parallel-replicas
-- no-parallel-replicas: use_skip_indexes_on_data_read is not supported with parallel replicas.

-- { echo ON }

SET use_skip_indexes_on_data_read = 1;

SET use_skip_indexes = 1;

SET use_query_condition_cache=0;

SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability=0;


DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    `id` Int64,
    `trace_id` FixedString(16) CODEC(ZSTD(1)),
    `text` String,
    INDEX bf_trace_id trace_id TYPE bloom_filter(0.001) GRANULARITY 1,
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4,index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

insert into tab select 100, unhex('31E5C3CAFA300A8DE5A84B740E2F2DB0'), 'aaa';

SELECT id,text FROM tab WHERE trace_id = unhex('31E5C3CAFA300A8DE5A84B740E2F2DB0');

