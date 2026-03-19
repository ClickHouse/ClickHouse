-- Regression test: SaveSubqueryResultToBufferTransform was saving shared ColumnPtrs to the
-- buffer. When the downstream pipeline modified shared sub-columns, the buffer's columns
-- were corrupted, causing ColumnVariant inconsistency during cross join compress/decompress.

SET allow_experimental_dynamic_type = 1;
SET allow_experimental_variant_type = 1;
SET use_variant_as_common_type = 1;
SET cross_join_min_rows_to_compress = 1;
SET cross_join_min_bytes_to_compress = 1;
SET max_threads = 4;

CREATE TABLE test_cv_compress (`id` Nullable(UInt64), `d` Dynamic(max_types = 133)) ENGINE = Memory;

INSERT INTO test_cv_compress SELECT number, number FROM numbers(100000);
INSERT INTO test_cv_compress SELECT number, arrayMap(x -> multiIf((number % 9) = 0, NULL, (number % 9) = 3, concat('str_', toString(number)), number), range((number % 10) + 1)) FROM numbers(200000, 100000);
INSERT INTO test_cv_compress SELECT number, multiIf((number % 4) = 3, concat('str_', toString(number)), (number % 4) = 2, NULL, (number % 4) = 1, number, arrayMap(x -> multiIf((number % 9) = 0, NULL, (number % 9) = 3, concat('str_', toString(number)), number), range((number % 10) + 1))) FROM numbers(400000, 400000);
INSERT INTO test_cv_compress SELECT number, if((number % 5) = 1, CAST([range(CAST((number % 10) + 1, 'UInt64'))], 'Array(Array(Dynamic))'), number) FROM numbers(100000, 100000);

SELECT count(equals(toInt256(257), intDiv(65536, -2147483649))), toInt128(2147483647) FROM test_cv_compress WHERE NOT empty((SELECT d.`Array(Variant(String, UInt64))`));

DROP TABLE test_cv_compress;
