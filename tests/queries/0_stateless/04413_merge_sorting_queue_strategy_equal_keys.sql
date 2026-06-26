DROP TABLE IF EXISTS equal_keys_default_04413;
DROP TABLE IF EXISTS equal_keys_batch_04413;

CREATE TABLE equal_keys_default_04413
(
    id UInt64,
    k UInt8,
    payload String,
    values Array(UInt32),
    n Nullable(Int64)
)
ENGINE = MergeTree
ORDER BY k
SETTINGS
    merge_sorting_queue_strategy = 'default',
    enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    index_granularity = 8,
    merge_max_block_size = 64,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0;

CREATE TABLE equal_keys_batch_04413 AS equal_keys_default_04413
ENGINE = MergeTree
ORDER BY k
SETTINGS
    merge_sorting_queue_strategy = 'batch',
    enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    index_granularity = 8,
    merge_max_block_size = 64,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0;

-- Populate both tables identically from numbers() so that the parts are physically
-- identical (same rows, same per-part insertion order, same part boundaries). The only
-- difference between the tables is merge_sorting_queue_strategy, so the equal-key order
-- comparison below isolates the merge behaviour and does not depend on read order.
INSERT INTO equal_keys_default_04413
SELECT
    number,
    toUInt8(number % 3),
    concat('payload-', toString(number), '-', toString(intHash64(number))),
    arrayMap(x -> toUInt32(number + x), range(toUInt8(number % 5))),
    if(number % 6 = 0, NULL, toInt64(number) * -7)
FROM numbers(240)
WHERE number % 4 = 0;

INSERT INTO equal_keys_default_04413
SELECT
    number,
    toUInt8(number % 3),
    concat('payload-', toString(number), '-', toString(intHash64(number))),
    arrayMap(x -> toUInt32(number + x), range(toUInt8(number % 5))),
    if(number % 6 = 0, NULL, toInt64(number) * -7)
FROM numbers(240)
WHERE number % 4 = 1;

INSERT INTO equal_keys_default_04413
SELECT
    number,
    toUInt8(number % 3),
    concat('payload-', toString(number), '-', toString(intHash64(number))),
    arrayMap(x -> toUInt32(number + x), range(toUInt8(number % 5))),
    if(number % 6 = 0, NULL, toInt64(number) * -7)
FROM numbers(240)
WHERE number % 4 = 2;

INSERT INTO equal_keys_default_04413
SELECT
    number,
    toUInt8(number % 3),
    concat('payload-', toString(number), '-', toString(intHash64(number))),
    arrayMap(x -> toUInt32(number + x), range(toUInt8(number % 5))),
    if(number % 6 = 0, NULL, toInt64(number) * -7)
FROM numbers(240)
WHERE number % 4 = 3;

INSERT INTO equal_keys_batch_04413
SELECT
    number,
    toUInt8(number % 3),
    concat('payload-', toString(number), '-', toString(intHash64(number))),
    arrayMap(x -> toUInt32(number + x), range(toUInt8(number % 5))),
    if(number % 6 = 0, NULL, toInt64(number) * -7)
FROM numbers(240)
WHERE number % 4 = 0;

INSERT INTO equal_keys_batch_04413
SELECT
    number,
    toUInt8(number % 3),
    concat('payload-', toString(number), '-', toString(intHash64(number))),
    arrayMap(x -> toUInt32(number + x), range(toUInt8(number % 5))),
    if(number % 6 = 0, NULL, toInt64(number) * -7)
FROM numbers(240)
WHERE number % 4 = 1;

INSERT INTO equal_keys_batch_04413
SELECT
    number,
    toUInt8(number % 3),
    concat('payload-', toString(number), '-', toString(intHash64(number))),
    arrayMap(x -> toUInt32(number + x), range(toUInt8(number % 5))),
    if(number % 6 = 0, NULL, toInt64(number) * -7)
FROM numbers(240)
WHERE number % 4 = 2;

INSERT INTO equal_keys_batch_04413
SELECT
    number,
    toUInt8(number % 3),
    concat('payload-', toString(number), '-', toString(intHash64(number))),
    arrayMap(x -> toUInt32(number + x), range(toUInt8(number % 5))),
    if(number % 6 = 0, NULL, toInt64(number) * -7)
FROM numbers(240)
WHERE number % 4 = 3;

OPTIMIZE TABLE equal_keys_default_04413 FINAL SETTINGS optimize_throw_if_noop = 1;
OPTIMIZE TABLE equal_keys_batch_04413 FINAL SETTINGS optimize_throw_if_noop = 1;

SELECT throwIf(
    (
        SELECT groupArray(tuple(id, k, payload, values, n))
        FROM (SELECT id, k, payload, values, n FROM equal_keys_default_04413 ORDER BY k, _part_offset)
    ) != (
        SELECT groupArray(tuple(id, k, payload, values, n))
        FROM (SELECT id, k, payload, values, n FROM equal_keys_batch_04413 ORDER BY k, _part_offset)
    ),
    'Equal-key merge order differs between default and batch sorting queue strategies')
FORMAT Null;

SELECT 'equal keys ok';

DROP TABLE equal_keys_default_04413;
DROP TABLE equal_keys_batch_04413;
