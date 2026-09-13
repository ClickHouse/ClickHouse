SET max_threads = 1;
SET optimize_move_to_prewhere = 0;

DROP TABLE IF EXISTS prewhere_uniform_filter;

CREATE TABLE prewhere_uniform_filter
(
    id UInt64,
    all_pass UInt8,
    all_drop UInt8,
    mixed UInt8,
    tail UInt8,
    almost_all UInt8,
    nullable_filter Nullable(UInt8),
    payload UInt64
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4;

INSERT INTO prewhere_uniform_filter
SELECT
    number,
    2,
    0,
    toUInt8(number % 2),
    toUInt8(number % 4 < 2),
    toUInt8(number != 9),
    if(number % 3 = 0, NULL, toUInt8(1)),
    number + 1
FROM numbers(10);

SELECT count(), sum(payload) FROM prewhere_uniform_filter PREWHERE all_pass;
SELECT count(), sum(payload) FROM prewhere_uniform_filter PREWHERE all_drop;
SELECT count(), sum(payload) FROM prewhere_uniform_filter PREWHERE mixed;
SELECT count(), sum(payload) FROM prewhere_uniform_filter PREWHERE tail;
SELECT count(), sum(payload) FROM prewhere_uniform_filter PREWHERE almost_all;
SELECT count(), sum(payload) FROM prewhere_uniform_filter PREWHERE nullable_filter;

DROP TABLE prewhere_uniform_filter;
