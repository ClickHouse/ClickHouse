SET enable_analyzer=1;
SET optimize_time_filter_with_preimage=1;

CREATE TABLE date_t__fuzz_0 (`id` UInt32, `value1` String, `date1` Date) ENGINE = ReplacingMergeTree ORDER BY id SETTINGS allow_nullable_key=1;

-- { echoOn }
EXPLAIN QUERY TREE run_passes = 1
SELECT *
FROM date_t__fuzz_0
WHERE ((toYear(date1) AS b) != toNullable(1993)) AND (id <= b);

EXPLAIN QUERY TREE run_passes = 1
SELECT *
FROM date_t__fuzz_0
WHERE ((toYear(date1) AS b) != 1993) AND (id <= b) SETTINGS optimize_time_filter_with_preimage=0;

EXPLAIN QUERY TREE run_passes = 1
SELECT *
FROM date_t__fuzz_0
WHERE ((toYear(date1) AS b) != 1993) AND (id <= b) SETTINGS optimize_time_filter_with_preimage=1;
