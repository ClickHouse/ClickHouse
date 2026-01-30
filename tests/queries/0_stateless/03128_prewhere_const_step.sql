CREATE TABLE account_test__fuzz_36 (`id` UInt32) ENGINE = MergeTree() PARTITION BY id % 64 ORDER BY id
SETTINGS index_granularity = 512, index_granularity_bytes = 0, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

INSERT INTO account_test__fuzz_36 SELECT * FROM generateRandom(1234) LIMIT 50000;

SELECT count() FROM account_test__fuzz_36 PREWHERE 1 AND id > 0;
SELECT count() FROM account_test__fuzz_36 PREWHERE 1 = 1 AND id > 0;
SELECT count() FROM account_test__fuzz_36 PREWHERE 1 = 1 AND 1 IN (SELECT 1) AND id > 0;
SELECT count() FROM account_test__fuzz_36 PREWHERE 1 = 1 AND 1 IN (SELECT number FROM numbers(10)) AND id > 0;
SELECT count() FROM account_test__fuzz_36 PREWHERE toNullable(1) AND id > 0;
SELECT count() FROM account_test__fuzz_36 PREWHERE materialize(1) AND id > 0;
