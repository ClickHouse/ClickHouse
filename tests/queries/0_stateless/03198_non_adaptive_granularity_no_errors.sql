DROP TABLE IF EXISTS data_02051__fuzz_24;

CREATE TABLE data_02051__fuzz_24 (`key` Int16, `value` String) ENGINE = MergeTree ORDER BY key SETTINGS index_granularity_bytes = 0, min_rows_for_wide_part = 0, min_bytes_for_wide_part=0  AS SELECT number, repeat(toString(number), 5) FROM numbers(1000000.);

SELECT  count(ignore(*)) FROM data_02051__fuzz_24 PREWHERE materialize(1) GROUP BY ignore(*);

detach table data_02051__fuzz_24;
attach table data_02051__fuzz_24;

SELECT  count(ignore(*)) FROM data_02051__fuzz_24 PREWHERE materialize(1) GROUP BY ignore(*);

DROP TABLE data_02051__fuzz_24;
