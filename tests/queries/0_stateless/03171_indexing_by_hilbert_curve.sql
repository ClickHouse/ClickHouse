-- Prevent remote replicas from skipping index analysis in Parallel Replicas. Otherwise, they may return full ranges and trigger max_rows_to_read validation failures.
SET parallel_replicas_index_analysis_only_on_coordinator = 0;
SET use_primary_key = 1; -- test relies on PK index being active (force_primary_key + max_rows_to_read)

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

-- A part holding a single distinct point must not be skipped

SET max_rows_to_read = 0, force_primary_key = 0;
-- Column statistics and skip indexes eliminate a part before the primary key is consulted, so leaving
-- them on would let the assertions below pass without the curve index being exercised at all.
SET use_statistics_for_part_pruning = 0, use_skip_indexes = 0;

DROP TABLE IF EXISTS test_hilbert_encode_zero;

CREATE TABLE test_hilbert_encode_zero (x UInt32, y UInt32) ENGINE = MergeTree ORDER BY hilbertEncode(x, y);
INSERT INTO test_hilbert_encode_zero SELECT 0, 0 FROM numbers(1000);

SELECT count() FROM test_hilbert_encode_zero WHERE x = 0 AND y = 0 SETTINGS analyze_index_with_space_filling_curves = 1, use_lightweight_primary_key_index_analysis = 1;
SELECT count() FROM test_hilbert_encode_zero WHERE x = 0 AND y = 0 SETTINGS analyze_index_with_space_filling_curves = 1, use_lightweight_primary_key_index_analysis = 0;
SELECT count() FROM test_hilbert_encode_zero WHERE x <= 3 AND y <= 3 SETTINGS analyze_index_with_space_filling_curves = 1, use_lightweight_primary_key_index_analysis = 1;
SELECT count() FROM test_hilbert_encode_zero WHERE x <= 3 AND y <= 3 SETTINGS analyze_index_with_space_filling_curves = 1, use_lightweight_primary_key_index_analysis = 0;
SELECT count() FROM test_hilbert_encode_zero WHERE x = 0 AND y = 0 SETTINGS analyze_index_with_space_filling_curves = 0;

-- ... while a condition that point cannot satisfy is still skipped, and it is the curve index that skips it

SELECT count() FROM test_hilbert_encode_zero WHERE x = 100 AND y = 100 SETTINGS analyze_index_with_space_filling_curves = 1, force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM (EXPLAIN indexes = 1 SELECT count() FROM test_hilbert_encode_zero WHERE x = 100 AND y = 100 SETTINGS analyze_index_with_space_filling_curves = 1, force_primary_key = 1) WHERE explain ILIKE '%Statistics%';
SELECT count() FROM (EXPLAIN indexes = 1 SELECT count() FROM test_hilbert_encode_zero WHERE x = 100 AND y = 100 SETTINGS analyze_index_with_space_filling_curves = 1, force_primary_key = 1) WHERE explain ILIKE '%hilbertEncode(x, y) has args in%';
SELECT count() FROM (EXPLAIN indexes = 1 SELECT count() FROM test_hilbert_encode_zero WHERE x = 100 AND y = 100 SETTINGS analyze_index_with_space_filling_curves = 1, force_primary_key = 1) WHERE explain ILIKE '%Parts: 0/1%';

DROP TABLE test_hilbert_encode_zero;

-- Single-point parts away from the origin, and a part that merely contains the origin

CREATE TABLE test_hilbert_encode_zero (x UInt32, y UInt32) ENGINE = MergeTree ORDER BY hilbertEncode(x, y);
INSERT INTO test_hilbert_encode_zero SELECT 0, 1 FROM numbers(1000);
SELECT count() FROM test_hilbert_encode_zero WHERE x = 0 AND y = 1 SETTINGS analyze_index_with_space_filling_curves = 1;
DROP TABLE test_hilbert_encode_zero;

CREATE TABLE test_hilbert_encode_zero (x UInt32, y UInt32) ENGINE = MergeTree ORDER BY hilbertEncode(x, y);
INSERT INTO test_hilbert_encode_zero SELECT 1, 0 FROM numbers(1000);
SELECT count() FROM test_hilbert_encode_zero WHERE x = 1 AND y = 0 SETTINGS analyze_index_with_space_filling_curves = 1;
DROP TABLE test_hilbert_encode_zero;

CREATE TABLE test_hilbert_encode_zero (x UInt32, y UInt32) ENGINE = MergeTree ORDER BY hilbertEncode(x, y);
INSERT INTO test_hilbert_encode_zero VALUES (0, 0), (1, 1), (2, 2), (3, 3);
SELECT count() FROM test_hilbert_encode_zero WHERE x = 0 AND y = 0 SETTINGS analyze_index_with_space_filling_curves = 1;
SELECT count() FROM test_hilbert_encode_zero WHERE x <= 3 AND y <= 3 SETTINGS analyze_index_with_space_filling_curves = 1;
DROP TABLE test_hilbert_encode_zero;
