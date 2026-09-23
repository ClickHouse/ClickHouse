SET allow_experimental_full_text_index = 1;
SET enable_time_time64_type = 1;
SET explain_query_plan_default = 'legacy';
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS json_all_values_text_time_types;

CREATE TABLE json_all_values_text_time_types
(
    data JSON(x DateTime, xs Array(DateTime64(3)), t Time64(3)),
    INDEX idx_values JSONAllValues(data) TYPE text(tokenizer = array) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 4;

INSERT INTO json_all_values_text_time_types
SELECT if(
    number < 4,
        '{"x":"2026-01-01 00:00:00","xs":["2026-01-01 00:00:00.000"],"t":"01:02:03.456"}',
        '{"x":"2026-01-01 12:00:00","xs":["2026-01-01 12:00:00.000"],"t":"12:34:56.789"}')
FROM numbers(8);

SET session_timezone = 'Europe/Moscow';

SELECT count() FROM json_all_values_text_time_types
WHERE data.x = toDateTime('2026-01-01 03:00:00');

SELECT count() FROM json_all_values_text_time_types
WHERE data.x = toDateTime('2026-01-01 03:00:00')
SETTINGS use_skip_indexes = 0;

SELECT count() FROM
(
    EXPLAIN indexes = 1
    SELECT count() FROM json_all_values_text_time_types
    WHERE data.x = toDateTime('2026-01-01 03:00:00')
)
WHERE explain LIKE '%idx_values%';

SELECT count() FROM json_all_values_text_time_types
WHERE has(data.xs, toDateTime64('2026-01-01 03:00:00.000', 3));

SELECT count() FROM json_all_values_text_time_types
WHERE has(data.xs, toDateTime64('2026-01-01 03:00:00.000', 3))
SETTINGS use_skip_indexes = 0;

SELECT count() FROM
(
    EXPLAIN indexes = 1
    SELECT count() FROM json_all_values_text_time_types
    WHERE has(data.xs, toDateTime64('2026-01-01 03:00:00.000', 3))
)
WHERE explain LIKE '%idx_values%';

SELECT count() FROM json_all_values_text_time_types
WHERE data.t = toTime64('01:02:03.456', 3);

SELECT count() FROM
(
    EXPLAIN indexes = 1
    SELECT count() FROM json_all_values_text_time_types
    WHERE data.t = toTime64('01:02:03.456', 3)
)
WHERE explain LIKE '%idx_values%';

SELECT count() FROM json_all_values_text_time_types
WHERE CAST(data.t AS String) = '01:02:03.456';

SELECT count() FROM
(
    EXPLAIN indexes = 1
    SELECT count() FROM json_all_values_text_time_types
    WHERE CAST(data.t AS String) = '01:02:03.456'
)
WHERE explain LIKE '%idx_values%';

DROP TABLE json_all_values_text_time_types;
