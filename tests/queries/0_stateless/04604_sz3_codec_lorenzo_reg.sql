-- Tags: no-fasttest, no-random-settings
-- no-fasttest: needs sz3 library
-- no-random-settings: a randomized `max_compress_block_size` that is not a multiple of the float width
-- is rejected by the SZ3 codec at write time

-- Regression test for the SZ3 `ALGO_LORENZO_REG` decompression path.
-- `RegressionPredictor::load` (used only by `ALGO_LORENZO_REG`) decremented `remaining_length` by the
-- uncompressed regression-coefficient count after the Huffman `decode`, instead of the bytes `decode`
-- actually consumed. The understated bound then made the following `encoder.load` reject valid data with
-- "SZ3 Huffman: encoded length exceeds compressed buffer", so a column written with
-- `CODEC(SZ3('ALGO_LORENZO_REG', ...))` could be inserted but failed every later read with CORRUPTED_DATA.

SET allow_experimental_codecs = 1;

DROP TABLE IF EXISTS tab_sz3_lorenzo_reg;

CREATE TABLE tab_sz3_lorenzo_reg (
    key  UInt64,
    orig Float64,
    val  Float64 CODEC(SZ3('ALGO_LORENZO_REG', 'ABS', 0.01))
) ENGINE = MergeTree ORDER BY key;

-- Stop background merges so the pre-merge assertion below sees the data after exactly one lossy pass.
-- Otherwise a background merge could fire between the second INSERT and that SELECT, recompress the
-- column through the lossy codec, and inflate the error past the one-pass bound, making the test flaky.
SYSTEM STOP MERGES tab_sz3_lorenzo_reg;

-- A smooth signal with a linear trend, so the regression predictor is actually selected for the blocks
-- (as opposed to the lossless fallback that SZ3 uses for tiny or incompressible inputs).
-- Two parts, so the OPTIMIZE FINAL below actually merges and re-reads the SZ3 column during the merge.
INSERT INTO tab_sz3_lorenzo_reg
SELECT number, number * 0.5 + sin(number * 0.1), number * 0.5 + sin(number * 0.1)
FROM numbers(50000);
INSERT INTO tab_sz3_lorenzo_reg
SELECT number, number * 0.5 + sin(number * 0.1), number * 0.5 + sin(number * 0.1)
FROM numbers(50000, 50000);

SELECT 'ALGO_LORENZO_REG data round-trips within the error bound (the read failed with CORRUPTED_DATA before the fix)';
-- The last column is a positive signal that the data really stayed on the lossy SZ3 path: the bit-exact
-- ALGO_LOSSLESS fallback (used when the lossy result does not win) would reproduce the input exactly and
-- would not exercise the fixed decompression path. The gtest SZ3Test.LorenzoRegSerializesAndDecodesRegressionCoefficients
-- additionally verifies that this data shape serializes a non-empty regression-coefficient stream.
SELECT count() = 100000, max(abs(orig - val)) <= 0.011, countIf(val != orig) > 0 FROM tab_sz3_lorenzo_reg;

SELECT 'The same holds after a merge recompresses the data';
SYSTEM START MERGES tab_sz3_lorenzo_reg;
OPTIMIZE TABLE tab_sz3_lorenzo_reg FINAL;
-- The OPTIMIZE FINAL merge decompresses and recompresses the column through the lossy codec, so the data
-- has now gone through two lossy passes: the initial write and this merge. Each pass can add up to the
-- ABS bound (0.01), and we keep a small extra margin on top for codec rounding.
SELECT count() = 100000, max(abs(orig - val)) <= 0.031 FROM tab_sz3_lorenzo_reg;

DROP TABLE tab_sz3_lorenzo_reg;
