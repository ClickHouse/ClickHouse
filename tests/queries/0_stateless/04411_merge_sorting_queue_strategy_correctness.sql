DROP TABLE IF EXISTS source_04411;
DROP TABLE IF EXISTS horizontal_default_04411;
DROP TABLE IF EXISTS horizontal_batch_04411;
DROP TABLE IF EXISTS vertical_default_04411;
DROP TABLE IF EXISTS vertical_batch_04411;

CREATE TABLE source_04411
(
    id UInt64,
    k UInt32,
    subkey UInt32,
    s String,
    lc LowCardinality(String),
    n Nullable(Int32),
    arr Array(UInt16)
)
ENGINE = Memory;

INSERT INTO source_04411
SELECT
    number AS id,
    toUInt32(number % 17) AS k,
    toUInt32(intHash32(number) % 11) AS subkey,
    concat('str-', toString(number % 37), '-', toString(intHash32(number))) AS s,
    concat('lc-', toString(number % 13)) AS lc,
    if(number % 7 = 0, NULL, toInt32(number) - 100) AS n,
    arrayMap(x -> toUInt16((number + x) % 1000), range(toUInt8(number % 4))) AS arr
FROM numbers(96);

CREATE TABLE horizontal_default_04411 AS source_04411
ENGINE = MergeTree
PARTITION BY tuple()
ORDER BY (k, subkey)
SETTINGS
    merge_use_batch_sorting_queue = 0,
    enable_vertical_merge_algorithm = 0,
    index_granularity = 8,
    index_granularity_bytes = 0,
    merge_max_block_size = 64,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0;

CREATE TABLE horizontal_batch_04411 AS source_04411
ENGINE = MergeTree
PARTITION BY tuple()
ORDER BY (k, subkey)
SETTINGS
    merge_use_batch_sorting_queue = 1,
    enable_vertical_merge_algorithm = 0,
    index_granularity = 8,
    index_granularity_bytes = 0,
    merge_max_block_size = 64,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0;

CREATE TABLE vertical_default_04411 AS source_04411
ENGINE = MergeTree
PARTITION BY tuple()
ORDER BY (k, subkey)
SETTINGS
    merge_use_batch_sorting_queue = 0,
    enable_vertical_merge_algorithm = 1,
    index_granularity = 8,
    index_granularity_bytes = 0,
    merge_max_block_size = 64,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0,
    vertical_merge_algorithm_min_columns_to_activate = 1;

CREATE TABLE vertical_batch_04411 AS source_04411
ENGINE = MergeTree
PARTITION BY tuple()
ORDER BY (k, subkey)
SETTINGS
    merge_use_batch_sorting_queue = 1,
    enable_vertical_merge_algorithm = 1,
    index_granularity = 8,
    index_granularity_bytes = 0,
    merge_max_block_size = 64,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0,
    vertical_merge_algorithm_min_columns_to_activate = 1;

INSERT INTO horizontal_default_04411 SELECT * FROM source_04411 WHERE id % 3 = 0;
INSERT INTO horizontal_default_04411 SELECT * FROM source_04411 WHERE id % 3 = 1;
INSERT INTO horizontal_default_04411 SELECT * FROM source_04411 WHERE id % 3 = 2;

INSERT INTO horizontal_batch_04411 SELECT * FROM source_04411 WHERE id % 3 = 0;
INSERT INTO horizontal_batch_04411 SELECT * FROM source_04411 WHERE id % 3 = 1;
INSERT INTO horizontal_batch_04411 SELECT * FROM source_04411 WHERE id % 3 = 2;

INSERT INTO vertical_default_04411 SELECT * FROM source_04411 WHERE id % 3 = 0;
INSERT INTO vertical_default_04411 SELECT * FROM source_04411 WHERE id % 3 = 1;
INSERT INTO vertical_default_04411 SELECT * FROM source_04411 WHERE id % 3 = 2;

INSERT INTO vertical_batch_04411 SELECT * FROM source_04411 WHERE id % 3 = 0;
INSERT INTO vertical_batch_04411 SELECT * FROM source_04411 WHERE id % 3 = 1;
INSERT INTO vertical_batch_04411 SELECT * FROM source_04411 WHERE id % 3 = 2;

OPTIMIZE TABLE horizontal_default_04411 FINAL SETTINGS optimize_throw_if_noop = 1;
OPTIMIZE TABLE horizontal_batch_04411 FINAL SETTINGS optimize_throw_if_noop = 1;
OPTIMIZE TABLE vertical_default_04411 FINAL SETTINGS optimize_throw_if_noop = 1;
OPTIMIZE TABLE vertical_batch_04411 FINAL SETTINGS optimize_throw_if_noop = 1;

SELECT throwIf(
    (
        SELECT groupArray(tuple(*))
        FROM (SELECT * FROM horizontal_default_04411 ORDER BY k, subkey, id)
    ) != (
        SELECT groupArray(tuple(*))
        FROM (SELECT * FROM horizontal_batch_04411 ORDER BY k, subkey, id)
    ),
    'Horizontal merge results differ between default and batch sorting queue strategies')
FORMAT Null;

SELECT throwIf(
    (
        SELECT groupArray(tuple(*))
        FROM (SELECT * FROM vertical_default_04411 ORDER BY k, subkey, id)
    ) != (
        SELECT groupArray(tuple(*))
        FROM (SELECT * FROM vertical_batch_04411 ORDER BY k, subkey, id)
    ),
    'Vertical merge results differ between default and batch sorting queue strategies')
FORMAT Null;

SELECT 'horizontal ok';
SELECT 'vertical ok';

DROP TABLE horizontal_default_04411;
DROP TABLE horizontal_batch_04411;
DROP TABLE vertical_default_04411;
DROP TABLE vertical_batch_04411;
DROP TABLE source_04411;
