DROP TABLE IF EXISTS vertical_granularity_default_04412;
DROP TABLE IF EXISTS vertical_granularity_batch_04412;

CREATE TABLE vertical_granularity_default_04412
(
    id UInt64,
    k UInt64,
    s String,
    a Array(UInt32),
    n Nullable(Int32),
    d Decimal(18, 4)
)
ENGINE = MergeTree
ORDER BY (k, id)
SETTINGS
    merge_sorting_queue_strategy = 'default',
    enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    index_granularity = 8,
    index_granularity_bytes = 10485760,
    merge_max_block_size = 64,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    use_const_adaptive_granularity = 0;

CREATE TABLE vertical_granularity_batch_04412 AS vertical_granularity_default_04412
ENGINE = MergeTree
ORDER BY (k, id)
SETTINGS
    merge_sorting_queue_strategy = 'batch',
    enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    index_granularity = 8,
    index_granularity_bytes = 10485760,
    merge_max_block_size = 64,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    use_const_adaptive_granularity = 0;

INSERT INTO vertical_granularity_default_04412
SELECT
    number,
    intHash64(number) % 17,
    concat('s-', toString(number), '-', toString(intHash32(number))),
    [toUInt32(number % 11), toUInt32(number % 13)],
    if(number % 5 = 0, NULL, toInt32(number) - 50),
    toDecimal64(toInt64(number) - 128, 4)
FROM numbers(257)
WHERE number % 3 = 0;

INSERT INTO vertical_granularity_default_04412
SELECT
    number,
    intHash64(number) % 17,
    concat('s-', toString(number), '-', toString(intHash32(number))),
    [toUInt32(number % 11), toUInt32(number % 13)],
    if(number % 5 = 0, NULL, toInt32(number) - 50),
    toDecimal64(toInt64(number) - 128, 4)
FROM numbers(257)
WHERE number % 3 = 1;

INSERT INTO vertical_granularity_default_04412
SELECT
    number,
    intHash64(number) % 17,
    concat('s-', toString(number), '-', toString(intHash32(number))),
    [toUInt32(number % 11), toUInt32(number % 13)],
    if(number % 5 = 0, NULL, toInt32(number) - 50),
    toDecimal64(toInt64(number) - 128, 4)
FROM numbers(257)
WHERE number % 3 = 2;

INSERT INTO vertical_granularity_batch_04412 SELECT * FROM vertical_granularity_default_04412 WHERE id % 3 = 0;
INSERT INTO vertical_granularity_batch_04412 SELECT * FROM vertical_granularity_default_04412 WHERE id % 3 = 1;
INSERT INTO vertical_granularity_batch_04412 SELECT * FROM vertical_granularity_default_04412 WHERE id % 3 = 2;

OPTIMIZE TABLE vertical_granularity_default_04412 FINAL SETTINGS optimize_throw_if_noop = 1;
OPTIMIZE TABLE vertical_granularity_batch_04412 FINAL SETTINGS optimize_throw_if_noop = 1;

SELECT throwIf(
    (
        SELECT groupArray(tuple(*))
        FROM (SELECT * FROM vertical_granularity_default_04412 ORDER BY k, id)
    ) != (
        SELECT groupArray(tuple(*))
        FROM (SELECT * FROM vertical_granularity_batch_04412 ORDER BY k, id)
    ),
    'Vertical merge results differ with adaptive granularity')
FORMAT Null;

SELECT 'vertical granularity ok';

DROP TABLE vertical_granularity_default_04412;
DROP TABLE vertical_granularity_batch_04412;
