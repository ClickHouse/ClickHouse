-- Test for ORDER BY ... LIMIT n optimization (top-K) - Test with index granularity > 1 and multiple columns in ORDER BY
-- Tags: long, no-tsan, no-asan, no-msan, no-s3-storage

DROP TABLE IF EXISTS tab1;

CREATE TABLE tab1
(
    id UInt32,
    v1 DateTime,
    v2 UInt32,
    INDEX v1idx v1 TYPE minmax GRANULARITY 1
) Engine = MergeTree ORDER BY id SETTINGS index_granularity = 64, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, max_bytes_to_merge_at_max_space_in_pool = 1, use_const_adaptive_granularity = 1, index_granularity_bytes = 0;

INSERT INTO tab1 SELECT rand(), toUnixTimestamp(toDateTime(number + 1) % 10), number + 1 from numbers(1000000);

SELECT 'Tests with multiple columns in ORDER BY';

SELECT toUnixTimestamp(v1), v2 FROM tab1 ORDER BY v1 DESC, v2 DESC LIMIT 5 SETTINGS  use_skip_indexes_for_top_k=1;
SELECT toUnixTimestamp(v1), v2 FROM tab1 ORDER BY v1 DESC, v2 DESC LIMIT 20 SETTINGS use_skip_indexes_for_top_k=1;
SELECT toUnixTimestamp(v1), v2 FROM tab1 WHERE v2 > 100000 ORDER BY v1 DESC, v2 DESC LIMIT 10 SETTINGS use_skip_indexes_on_data_read=1, use_skip_indexes_for_top_k=1;

SELECT toUnixTimestamp(v1), v2 FROM tab1 ORDER BY v1 ASC, v2 ASC LIMIT 5 SETTINGS  use_skip_indexes_for_top_k=1;
SELECT toUnixTimestamp(v1), v2 FROM tab1 ORDER BY v1 ASC, v2 ASC LIMIT 20 SETTINGS use_skip_indexes_for_top_k=1;
SELECT toUnixTimestamp(v1), v2 FROM tab1 WHERE v2 > 100000 ORDER BY v1 ASC, v2 ASC LIMIT 10 SETTINGS use_skip_indexes_on_data_read=1, use_skip_indexes_for_top_k=1;

SELECT 'Tests with index granularity > 1';

DROP TABLE tab1;

CREATE TABLE tab1
(
    id UInt32,
    v1 DateTime,
    v2 UInt32,
    INDEX v1idx v1 TYPE minmax GRANULARITY 4
) Engine = MergeTree ORDER BY id SETTINGS index_granularity = 64, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, max_bytes_to_merge_at_max_space_in_pool = 1, use_const_adaptive_granularity = 1, index_granularity_bytes = 0;

INSERT INTO tab1 SELECT rand(), toUnixTimestamp(toDateTime(number + 1) % 10), number + 1 from numbers(1000000);

SELECT toUnixTimestamp(v1), v2 FROM tab1 ORDER BY v1 DESC, v2 DESC LIMIT 5 SETTINGS  use_skip_indexes_for_top_k=1;
SELECT toUnixTimestamp(v1), v2 FROM tab1 ORDER BY v1 DESC, v2 DESC LIMIT 20 SETTINGS use_skip_indexes_for_top_k=1;
SELECT toUnixTimestamp(v1), v2 FROM tab1 WHERE v2 > 100000 ORDER BY v1 DESC, v2 DESC LIMIT 10 SETTINGS use_skip_indexes_on_data_read=1, use_skip_indexes_for_top_k=1;

SELECT toUnixTimestamp(v1), v2 FROM tab1 ORDER BY v1 ASC, v2 ASC LIMIT 5 SETTINGS  use_skip_indexes_for_top_k=1;
SELECT toUnixTimestamp(v1), v2 FROM tab1 ORDER BY v1 ASC, v2 ASC LIMIT 20 SETTINGS use_skip_indexes_for_top_k=1;
SELECT toUnixTimestamp(v1), v2 FROM tab1 WHERE v2 > 100000 ORDER BY v1 ASC, v2 ASC LIMIT 10 SETTINGS use_skip_indexes_on_data_read=1, use_skip_indexes_for_top_k=1;
