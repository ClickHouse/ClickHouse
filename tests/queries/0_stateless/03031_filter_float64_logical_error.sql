CREATE TABLE 03031_test
(
    `id` UInt64,
    `value_1` String,
    `value_2` String,
    `value_3` String,
    INDEX value_1_idx value_1 TYPE bloom_filter GRANULARITY 1,
    INDEX value_2_idx value_2 TYPE ngrambf_v1(3, 512, 2, 0) GRANULARITY 1,
    INDEX value_3_idx value_3 TYPE tokenbf_v1(512, 3, 0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO 03031_test SELECT
    number,
    toString(number),
    toString(number),
    toString(number)
FROM numbers(10);

SELECT
    count('9223372036854775806'),
    7
FROM 03031_test
PREWHERE (id = NULL) AND 1024
WHERE 0.0001
GROUP BY '0.03'
    WITH ROLLUP
SETTINGS force_primary_key = 1, force_data_skipping_indices = 'value_1_idx, value_2_idx', enable_analyzer=0;


SELECT
    count('9223372036854775806'),
    7
FROM 03031_test
PREWHERE (id = NULL) AND 1024
WHERE 0.0001
GROUP BY '0.03'
    WITH ROLLUP
SETTINGS force_primary_key = 1, force_data_skipping_indices = 'value_1_idx, value_2_idx', enable_analyzer=1; -- { serverError ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER }
