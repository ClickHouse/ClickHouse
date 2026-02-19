-- Tags: no-parallel-replicas

DROP TABLE IF EXISTS t_lwd_indexes;

SET enable_lightweight_update = 1;
SET use_skip_indexes_on_data_read = 0;

CREATE TABLE t_lwd_indexes
(
    key UInt64,
    value String,
    INDEX idx_key (key) TYPE minmax GRANULARITY 1,
    INDEX idx_value (value) TYPE bloom_filter(0.001) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 128, index_granularity_bytes = '10M', min_bytes_for_wide_part = 0, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_lwd_indexes SELECT number, 'v' || toString(number) FROM numbers(10000);

DELETE FROM t_lwd_indexes WHERE key < 500 SETTINGS lightweight_delete_mode = 'alter_update';
DELETE FROM t_lwd_indexes WHERE key < 5000 SETTINGS lightweight_delete_mode = 'lightweight_update_force';

SELECT count() FROM t_lwd_indexes WHERE key = 1000 SETTINGS force_data_skipping_indices = 'idx_key';

SELECT trim(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM t_lwd_indexes WHERE key = 1000 SETTINGS force_data_skipping_indices = 'idx_key'
)
WHERE explain LIKE '%Granules%';

SELECT count() FROM t_lwd_indexes WHERE key = 9000 SETTINGS force_data_skipping_indices = 'idx_key';

SELECT trim(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM t_lwd_indexes WHERE key = 9000 SETTINGS force_data_skipping_indices = 'idx_key'
)
WHERE explain LIKE '%Granules%';

SELECT count() FROM t_lwd_indexes WHERE value = 'v1000' SETTINGS force_data_skipping_indices = 'idx_value';

SELECT trim(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM t_lwd_indexes WHERE value = 'v1000' SETTINGS force_data_skipping_indices = 'idx_value'
)
WHERE explain LIKE '%Granules%';

SELECT count() FROM t_lwd_indexes WHERE value = 'v9000' SETTINGS force_data_skipping_indices = 'idx_value';

SELECT trim(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM t_lwd_indexes WHERE value = 'v9000' SETTINGS force_data_skipping_indices = 'idx_value'
)
WHERE explain LIKE '%Granules%';

DROP TABLE t_lwd_indexes;
