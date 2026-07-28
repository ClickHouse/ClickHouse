-- Regression: a constant vector expression (e.g. `INSERT INTO t SELECT [1., ...] FROM numbers(N)`) can, in principle,
-- reach the `Quantized` serializer as a `ColumnConst(Array)`. The write path must materialize it rather than reject the
-- cast to `ColumnArray`. Covers a data-independent method (`int8`) and the trained `product` method (whose training also casts).

SET allow_experimental_codecs = 1;

DROP TABLE IF EXISTS quantize_const_flat;
CREATE TABLE quantize_const_flat (id UInt32, vec Array(Float32) CODEC(Quantized('int8', 8))) ENGINE = MergeTree ORDER BY id;
INSERT INTO quantize_const_flat SELECT number, [1., 2., 3., 4., 5., 6., 7., 8.] FROM numbers(10);
-- int8 code is dimensions + 4-byte norm = 12 bytes; every row encodes fine.
SELECT 'flat', count(), uniqExact(length(vec.quantized)) AS distinct_code_lengths FROM quantize_const_flat;
DROP TABLE quantize_const_flat;

DROP TABLE IF EXISTS quantize_const_pq;
CREATE TABLE quantize_const_pq (id UInt32, vec Array(Float32) CODEC(Quantized('product', 8, 4, 2))) ENGINE = MergeTree ORDER BY id;
INSERT INTO quantize_const_pq SELECT number, [1., 2., 3., 4., 5., 6., 7., 8.] FROM numbers(10);
-- pq code is m = 2 bytes (nbits = 4 <= 8 => one byte per subspace).
SELECT 'product', count(), uniqExact(length(vec.quantized)) AS distinct_code_lengths FROM quantize_const_pq;
DROP TABLE quantize_const_pq;
