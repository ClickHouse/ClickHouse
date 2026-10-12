SET check_query_single_value_result = 1;

CREATE TABLE packed_finalization
(
    p UInt64,
    k UInt64,
    v UInt64,
    PROJECTION totals (SELECT p, sum(v) GROUP BY p)
)
ENGINE = MergeTree
PARTITION BY p ORDER BY k
SETTINGS min_bytes_for_full_part_storage = '32M', fsync_after_insert = 1,
    non_replicated_deduplication_window = 10;

INSERT INTO packed_finalization SELECT number % 4, number, number FROM numbers(40)
SETTINGS max_threads = 1, max_insert_threads = 1,
    max_insert_delayed_streams_for_parallel_write = 1000, deduplicate_insert = 'enable',
    insert_deduplication_token = 'packed-finalization';
INSERT INTO packed_finalization SELECT number % 4, number, number FROM numbers(40)
SETTINGS max_threads = 1, max_insert_threads = 1,
    max_insert_delayed_streams_for_parallel_write = 1000, deduplicate_insert = 'enable',
    insert_deduplication_token = 'packed-finalization';

SELECT count(), sum(v) FROM packed_finalization;
SELECT p, sum(v) FROM packed_finalization GROUP BY p ORDER BY p
SETTINGS force_optimize_projection = 1;
CHECK TABLE packed_finalization;

INSERT INTO packed_finalization SELECT number % 4, number + 40, number FROM numbers(40)
SETTINGS max_threads = 1, max_insert_threads = 1,
    max_insert_delayed_streams_for_parallel_write = 1000;
OPTIMIZE TABLE packed_finalization FINAL;
SELECT count(), sum(v) FROM packed_finalization;
CHECK TABLE packed_finalization;

DROP TABLE packed_finalization;
