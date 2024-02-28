CREATE TABLE data_02052_1_wide0__fuzz_48
(
    `key` Nullable(Int64),
    `value` UInt8
)
    ENGINE = MergeTree
        ORDER BY key
        SETTINGS min_bytes_for_wide_part = 0, allow_nullable_key = 1 AS
SELECT
    number,
    repeat(toString(number), 5)
FROM numbers(1);

SELECT * APPLY max
FROM data_02052_1_wide0__fuzz_48
GROUP BY toFixedString(toFixedString(toFixedString(toFixedString(toFixedString(toLowCardinality('UInt256'), toFixedString(toNullable(toNullable(2)), toFixedString(toFixedString(7), 7)), 7), 7), materialize(toNullable(7))), 7), materialize(7))
WITH CUBE
    SETTINGS max_read_buffer_size = 7, max_threads = 9223372036854775807; -- { serverError INVALID_SETTING_VALUE }

SELECT zero + 1 AS x
FROM system.zeros
    SETTINGS max_block_size = 9223372036854775806, max_rows_to_read = 20, read_overflow_mode = 'break'; -- { serverError INVALID_SETTING_VALUE }

EXPLAIN PIPELINE SELECT zero + 1 AS x FROM system.zeros SETTINGS max_block_size = 9223372036854775806, max_rows_to_read = 20, read_overflow_mode = 'break'; -- { serverError INVALID_SETTING_VALUE }
