-- Tags: long, no-object-storage

DROP TABLE IF EXISTS mt;

CREATE TABLE mt (x UInt64) ENGINE = MergeTree ORDER BY x
    SETTINGS cleanup_delay_period = 1, cleanup_delay_period_random_add = 0,
    cleanup_thread_preferred_points_per_iteration=0, old_parts_lifetime = 1, parts_to_delay_insert = 100000, parts_to_throw_insert = 100000;

SYSTEM STOP MERGES mt;

SET max_block_size = 1, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0, max_insert_delayed_streams_for_parallel_write = 1000, max_execution_time = 600;
INSERT INTO mt SELECT * FROM numbers(1000);
SET max_block_size = 65536;

SELECT count(), sum(x) FROM mt;

SYSTEM START MERGES mt;
OPTIMIZE TABLE mt FINAL;

SELECT count(), sum(x) FROM mt;

DROP TABLE mt;
