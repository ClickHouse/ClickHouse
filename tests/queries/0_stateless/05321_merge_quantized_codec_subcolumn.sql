-- The codes of a `Quantized` column read through a `Merge` table, from a table that does not have the column,
-- are the default of the codes type. Without the per-part codebook the codes of an in-memory value cannot be computed.

SET enable_quantized_codec = 1;

DROP TABLE IF EXISTS t_05321_dist_b;
DROP TABLE IF EXISTS t_05321_a_pq;
DROP TABLE IF EXISTS t_05321_a_int8;
DROP TABLE IF EXISTS t_05321_b;

CREATE TABLE t_05321_a_pq (id UInt32, vec Array(Float32) CODEC(Quantized('product', 8, 4, 2))) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_05321_a_int8 (id UInt32, vec Array(Float32) CODEC(Quantized('int8', 8))) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_05321_b (id UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_05321_dist_b AS t_05321_b ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_05321_b);

INSERT INTO t_05321_a_pq SELECT number, arrayMap(j -> toFloat32((number * 7 + j) % 13), range(8)) FROM numbers(32);
INSERT INTO t_05321_a_int8 SELECT number, arrayMap(j -> toFloat32((number * 7 + j) % 13), range(8)) FROM numbers(32);
INSERT INTO t_05321_b VALUES (100), (101);

SELECT varSamp(cityHash64(vec.quantized)) FROM merge(currentDatabase(), '^t_05321_(a_pq|b)$') FINAL FORMAT Null;
SELECT id, toTypeName(vec.quantized), hex(vec.quantized), vec.size0 FROM merge(currentDatabase(), '^t_05321_(a_pq|b)$') WHERE id >= 100 ORDER BY id;
SELECT id, vec.product_quantization_codebook = defaultValueOfTypeName(toTypeName(vec.product_quantization_codebook)) FROM merge(currentDatabase(), '^t_05321_(a_pq|b)$') WHERE id >= 100 ORDER BY id;
SELECT count() FROM merge(currentDatabase(), '^t_05321_(a_pq|b)$') WHERE NOT ignore(vec.quantized, vec.product_quantization_codebook);
SELECT id, toTypeName(vec.quantized), hex(vec.quantized) FROM merge(currentDatabase(), '^t_05321_(a_int8|b)$') WHERE id >= 100 ORDER BY id;
SELECT id, hex(vec.quantized) FROM merge(currentDatabase(), '^t_05321_(a_pq|dist_b)$') WHERE id = 100;

SELECT getSubcolumn(materialize(vec), 'quantized') FROM t_05321_a_pq FORMAT Null; -- { serverError NOT_IMPLEMENTED }

DROP TABLE t_05321_dist_b;
DROP TABLE t_05321_a_pq;
DROP TABLE t_05321_a_int8;
DROP TABLE t_05321_b;
