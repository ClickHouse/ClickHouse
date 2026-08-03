-- Tags: no-fasttest

-- Test for Parquet bloom filter with empty IN set
-- This test verifies that empty IN clauses don't cause assertion failures
-- when bloom filter push down is enabled.

-- First, create a Parquet file with bloom filter
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04056_parquet_bloom_filter_empty_in.parquet', 'Parquet')
SELECT
    number as id,
    toString(number) as name,
    number % 10 as category
FROM numbers(1000)
SETTINGS
    output_format_parquet_write_bloom_filter = 1,
    engine_file_truncate_on_insert = 1;

-- Test 1: Query with empty IN clause using the parquet file with bloom filter
-- This should not cause an assertion failure
SELECT count(*) FROM file(currentDatabase() || '_04056_parquet_bloom_filter_empty_in.parquet', 'Parquet')
WHERE id IN ()
SETTINGS input_format_parquet_bloom_filter_push_down = 1, input_format_parquet_filter_push_down = 0;

-- Test 2: Another empty IN case
SELECT count(*) FROM file(currentDatabase() || '_04056_parquet_bloom_filter_empty_in.parquet', 'Parquet')
WHERE name IN ()
SETTINGS input_format_parquet_bloom_filter_push_down = 1, input_format_parquet_filter_push_down = 0;

-- Test 3: Empty IN with NOT IN
SELECT count(*) FROM file(currentDatabase() || '_04056_parquet_bloom_filter_empty_in.parquet', 'Parquet')
WHERE id NOT IN ()
SETTINGS input_format_parquet_bloom_filter_push_down = 1, input_format_parquet_filter_push_down = 0;

-- Test 4: Multiple conditions with empty IN
SELECT count(*) FROM file(currentDatabase() || '_04056_parquet_bloom_filter_empty_in.parquet', 'Parquet')
WHERE category = 5 AND id IN ()
SETTINGS input_format_parquet_bloom_filter_push_down = 1, input_format_parquet_filter_push_down = 0;
