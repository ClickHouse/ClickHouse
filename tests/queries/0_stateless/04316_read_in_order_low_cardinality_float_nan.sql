-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/101838
-- read-in-order must treat LowCardinality(Float) sorting keys like plain Float keys,
-- so NaN (which sorts like NULL) is placed correctly under every NULLS direction.

-- { echo }
SET optimize_read_in_order = 1;
SET max_threads = 1;
SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS test_lc_float_nan;
CREATE TABLE test_lc_float_nan (c0 LowCardinality(Float64)) ENGINE = MergeTree() ORDER BY c0;
INSERT INTO test_lc_float_nan VALUES (0), (nan), (1);

SELECT * FROM test_lc_float_nan ORDER BY c0 ASC NULLS FIRST;
SELECT * FROM test_lc_float_nan ORDER BY c0 ASC NULLS LAST;
SELECT * FROM test_lc_float_nan ORDER BY c0 DESC NULLS FIRST;
SELECT * FROM test_lc_float_nan ORDER BY c0 DESC NULLS LAST;

DROP TABLE IF EXISTS test_lc_float32_nan;
CREATE TABLE test_lc_float32_nan (c0 LowCardinality(Float32)) ENGINE = MergeTree() ORDER BY c0;
INSERT INTO test_lc_float32_nan VALUES (0), (nan), (1);

SELECT * FROM test_lc_float32_nan ORDER BY c0 ASC NULLS FIRST;
SELECT * FROM test_lc_float32_nan ORDER BY c0 ASC NULLS LAST;
SELECT * FROM test_lc_float32_nan ORDER BY c0 DESC NULLS FIRST;
SELECT * FROM test_lc_float32_nan ORDER BY c0 DESC NULLS LAST;

DROP TABLE IF EXISTS test_lc_bfloat16_nan;
CREATE TABLE test_lc_bfloat16_nan (c0 LowCardinality(BFloat16)) ENGINE = MergeTree() ORDER BY c0;
INSERT INTO test_lc_bfloat16_nan VALUES (0), (nan), (1);

SELECT * FROM test_lc_bfloat16_nan ORDER BY c0 ASC NULLS FIRST;
SELECT * FROM test_lc_bfloat16_nan ORDER BY c0 ASC NULLS LAST;
SELECT * FROM test_lc_bfloat16_nan ORDER BY c0 DESC NULLS FIRST;
SELECT * FROM test_lc_bfloat16_nan ORDER BY c0 DESC NULLS LAST;

DROP TABLE test_lc_float_nan;
DROP TABLE test_lc_float32_nan;
DROP TABLE test_lc_bfloat16_nan;
