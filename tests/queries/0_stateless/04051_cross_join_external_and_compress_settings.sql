-- Tags: no-random-settings

-- Test max_bytes_before_compress_cross_join setting
SET max_bytes_before_compress_cross_join = 1;
SELECT count() FROM (SELECT number FROM numbers(100)) t1 CROSS JOIN (SELECT number FROM numbers(100)) t2;

-- Test max_bytes_before_external_cross_join setting (requires tmp storage, which is available by default)
SET max_bytes_before_compress_cross_join = 0;
SET max_bytes_before_external_cross_join = 1;
SELECT count() FROM (SELECT number FROM numbers(100)) t1 CROSS JOIN (SELECT number FROM numbers(100)) t2;

-- Test both settings together
SET max_bytes_before_compress_cross_join = 1;
SET max_bytes_before_external_cross_join = 100000000;
SELECT count() FROM (SELECT number FROM numbers(100)) t1 CROSS JOIN (SELECT number FROM numbers(100)) t2;

-- Test with zero values (disabled)
SET max_bytes_before_compress_cross_join = 0;
SET max_bytes_before_external_cross_join = 0;
SELECT count() FROM (SELECT number FROM numbers(100)) t1 CROSS JOIN (SELECT number FROM numbers(100)) t2;

-- Verify results are correct with larger data and external spill
SET max_bytes_before_external_cross_join = 1;
SELECT sum(t1.number + t2.number) FROM (SELECT number FROM numbers(50)) t1 CROSS JOIN (SELECT number FROM numbers(50)) t2;
