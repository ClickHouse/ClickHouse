-- With a reversed (DESC) Nullable key column, NULL sorts as the greatest value: NULLs are stored at
-- the physical beginning of the part and the primary key index maps them to +Inf on the value axis.
-- For the final granule of a part without a final mark (non-adaptive granularity,
-- `index_granularity_bytes = 0`) the index analysis must not treat a NULL first value as a proof
-- that the granule holds a single key: that holds only for non-reversed columns, where NULLs are
-- stored physically last. Getting it wrong poisoned whole-part ranges and produced wrong counts in
-- both directions.

-- The part starts with the NULLs, and one granule spans {NULL, 5}.
DROP TABLE IF EXISTS test_reverse_nullable_no_final_mark;
CREATE TABLE test_reverse_nullable_no_final_mark (x Nullable(UInt64)) ENGINE = MergeTree
ORDER BY x DESC
SETTINGS allow_nullable_key = 1, index_granularity = 2, index_granularity_bytes = 0, add_minmax_index_for_numeric_columns = 0, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

INSERT INTO test_reverse_nullable_no_final_mark VALUES (5), (4), (3), (2), (1), (NULL), (NULL), (NULL);

SELECT count() FROM test_reverse_nullable_no_final_mark WHERE x IS NULL;
SELECT count() FROM test_reverse_nullable_no_final_mark WHERE x IS NULL SETTINGS use_primary_key = 0;
SELECT count() FROM test_reverse_nullable_no_final_mark WHERE x IS NOT NULL;
SELECT count() FROM test_reverse_nullable_no_final_mark WHERE x IS NOT NULL SETTINGS use_primary_key = 0;
SELECT count() FROM test_reverse_nullable_no_final_mark WHERE x = 1;
SELECT count() FROM test_reverse_nullable_no_final_mark WHERE x = 1 SETTINGS use_primary_key = 0;
SELECT count() FROM test_reverse_nullable_no_final_mark WHERE x = 5;
SELECT count() FROM test_reverse_nullable_no_final_mark WHERE x < 3;
SELECT count() FROM test_reverse_nullable_no_final_mark WHERE x >= 4;
SELECT count() FROM test_reverse_nullable_no_final_mark WHERE x IN (1, 5);

-- Both analysis paths must agree.
SELECT count() FROM test_reverse_nullable_no_final_mark WHERE x IS NULL SETTINGS use_lightweight_primary_key_index_analysis = 0;
SELECT count() FROM test_reverse_nullable_no_final_mark WHERE x = 1 SETTINGS use_lightweight_primary_key_index_analysis = 0;

DROP TABLE test_reverse_nullable_no_final_mark;

-- The part is small enough that the granule with the NULL is the first one: {NULL, 3}.
DROP TABLE IF EXISTS test_reverse_nullable_mixed_tail;
CREATE TABLE test_reverse_nullable_mixed_tail (x Nullable(UInt64)) ENGINE = MergeTree
ORDER BY x DESC
SETTINGS allow_nullable_key = 1, index_granularity = 2, index_granularity_bytes = 0, add_minmax_index_for_numeric_columns = 0, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

INSERT INTO test_reverse_nullable_mixed_tail VALUES (3), (2), (1), (NULL);

SELECT count() FROM test_reverse_nullable_mixed_tail WHERE x IS NULL;
SELECT count() FROM test_reverse_nullable_mixed_tail WHERE x IS NULL SETTINGS use_primary_key = 0;
SELECT count() FROM test_reverse_nullable_mixed_tail WHERE x IS NOT NULL;
SELECT count() FROM test_reverse_nullable_mixed_tail WHERE x = 1;
SELECT count() FROM test_reverse_nullable_mixed_tail WHERE x = 3;
SELECT count() FROM test_reverse_nullable_mixed_tail WHERE x IS NULL SETTINGS use_lightweight_primary_key_index_analysis = 0;

DROP TABLE test_reverse_nullable_mixed_tail;
