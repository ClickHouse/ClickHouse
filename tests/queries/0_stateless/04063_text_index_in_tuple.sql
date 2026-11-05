DROP TABLE IF EXISTS tab_in_preprocess;

CREATE TABLE tab_in_preprocess
(
    `id` UInt64,
    `str` String,
    INDEX idx_str str TYPE text(tokenizer = 'array', preprocessor = lower(str))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab_in_preprocess SELECT number, concat('Hello', toString(number)) FROM numbers(100);

SELECT
    count()
FROM tab_in_preprocess
WHERE (id, str) IN (
    SELECT tuple(number, 'Hello10') FROM numbers(100)
) SETTINGS force_data_skipping_indices = 'idx_str';

SELECT
    count()
FROM tab_in_preprocess
WHERE (id, str) IN (
    SELECT number, 'Hello10' FROM numbers(100)
) SETTINGS force_data_skipping_indices = 'idx_str';

SELECT
    count()
FROM tab_in_preprocess
WHERE (id, str) IN (
    SELECT number FROM numbers(100)
); -- { serverError TYPE_MISMATCH, NUMBER_OF_COLUMNS_DOESNT_MATCH, ILLEGAL_TYPE_OF_ARGUMENT }

SELECT
    count()
FROM tab_in_preprocess
WHERE (id, str) IN (
    SELECT tuple(number) FROM numbers(100)
); -- { serverError TYPE_MISMATCH, NUMBER_OF_COLUMNS_DOESNT_MATCH, ILLEGAL_TYPE_OF_ARGUMENT }

SELECT
    count()
FROM tab_in_preprocess
WHERE (id, str) IN (
    SELECT tuple(number, 'Hello10', 'Hello11') FROM numbers(100)
); -- { serverError TYPE_MISMATCH, NUMBER_OF_COLUMNS_DOESNT_MATCH, ILLEGAL_TYPE_OF_ARGUMENT }

SELECT
    count()
FROM tab_in_preprocess
WHERE (id, str) IN (
    SELECT number, 'Hello10', 'Hello11' FROM numbers(100)
); -- { serverError TYPE_MISMATCH, NUMBER_OF_COLUMNS_DOESNT_MATCH, ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE tab_in_preprocess;
