DROP TABLE IF EXISTS test_hilbert_encode_hilbert_encode;

CREATE TABLE test_hilbert_encode (x UInt32, y UInt32) ENGINE = MergeTree ORDER BY hilbertEncode(x, y) SETTINGS index_granularity = 8192, index_granularity_bytes = '1Mi';
INSERT INTO test_hilbert_encode SELECT number DIV 1024, number % 1024 FROM numbers(1048576);

set max_streams_for_merge_tree_reading = 1;

SET max_rows_to_read = 8192, force_primary_key = 1, analyze_index_with_space_filling_curves = 1;
SELECT count() FROM test_hilbert_encode WHERE x >= 10 AND x <= 20 AND y >= 20 AND y <= 30;

SET max_rows_to_read = 8192, force_primary_key = 1, analyze_index_with_space_filling_curves = 0;
SELECT count() FROM test_hilbert_encode WHERE x >= 10 AND x <= 20 AND y >= 20 AND y <= 30;  -- { serverError 277 }

DROP TABLE test_hilbert_encode;

-- The same, but with more precise index

CREATE TABLE test_hilbert_encode (x UInt32, y UInt32) ENGINE = MergeTree ORDER BY hilbertEncode(x, y) SETTINGS index_granularity = 1;
SET max_rows_to_read = 0;
INSERT INTO test_hilbert_encode SELECT number DIV 32, number % 32 FROM numbers(1024);

SET max_rows_to_read = 200, force_primary_key = 1, analyze_index_with_space_filling_curves = 1;
SELECT count() FROM test_hilbert_encode WHERE x >= 10 AND x <= 20 AND y >= 20 AND y <= 30;

-- Various other conditions

SELECT count() FROM test_hilbert_encode WHERE x = 10 SETTINGS max_rows_to_read = 49;
SELECT count() FROM test_hilbert_encode WHERE x = 10 AND y > 10 SETTINGS max_rows_to_read = 33;
SELECT count() FROM test_hilbert_encode WHERE x = 10 AND y < 10 SETTINGS max_rows_to_read = 15;

SELECT count() FROM test_hilbert_encode WHERE y = 10 SETTINGS max_rows_to_read = 50;
SELECT count() FROM test_hilbert_encode WHERE x >= 10 AND y = 10 SETTINGS max_rows_to_read = 35;
SELECT count() FROM test_hilbert_encode WHERE y = 10 AND x <= 10 SETTINGS max_rows_to_read = 17;

SELECT count() FROM test_hilbert_encode PREWHERE x >= 10 WHERE x < 11 AND y = 10 SETTINGS max_rows_to_read = 2;

DROP TABLE test_hilbert_encode;
