-- Tags: no-parallel-replicas
-- (the two-stage codes rewrite is disabled under parallel replicas, so the plan check below cannot hold there)
-- A mutation that rewrites a column with a `Quantized` codec writes the codec's streams, like an insert or a merge does:
-- the quantized codes, and the codebook of the `product` method.

SET enable_quantized_codec = 1;
SET mutations_sync = 1;

DROP TABLE IF EXISTS t_product;
DROP TABLE IF EXISTS t_rabitq;
DROP TABLE IF EXISTS t_rabitq_ref;
DROP TABLE IF EXISTS t_materialized;
DROP TABLE IF EXISTS t_dependency;
DROP TABLE IF EXISTS t_dependency_ref;
DROP TABLE IF EXISTS t_block_sizes;

-- `product` (always a Wide part).
CREATE TABLE t_product (id UInt64, v Array(Float32) CODEC(Quantized('product', 8, 2, 2)))
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_product SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(8)) FROM numbers(100);
ALTER TABLE t_product UPDATE v = arrayMap(x -> -x, v) WHERE id % 3 = 0;

SELECT 'product', has(substreams, 'v.quantized'), has(substreams, 'v.product_quantization_codebook')
FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_product' AND active AND column = 'v';
SELECT count(), sum(length(v.quantized)), countIf(length(v.product_quantization_codebook) = 128) FROM t_product;
SELECT countIf(explain ILIKE '%quantized shortlist%') > 0, countIf(explain ILIKE '%productQuantizationDistance%') > 0
FROM (EXPLAIN actions = 1 SELECT id FROM t_product ORDER BY L2Distance(v, [1., 1., 1., 1., 1., 1., 1., 1.]::Array(Float32)) LIMIT 5
      SETTINGS vector_search_use_quantized_codes = 1);
SELECT count() FROM (SELECT id FROM t_product ORDER BY L2Distance(v, [1., 1., 1., 1., 1., 1., 1., 1.]::Array(Float32)) LIMIT 5)
SETTINGS vector_search_use_quantized_codes = 1;

DETACH TABLE t_product;
ATTACH TABLE t_product;
SELECT count(), sum(length(v.quantized)), countIf(length(v.product_quantization_codebook) = 128) FROM t_product;

-- `rabitq` in a Compact part: the codes equal those of an inserted part with the same vectors.
CREATE TABLE t_rabitq (id UInt64, v Array(Float32) CODEC(Quantized('rabitq', 8)))
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = '1G', min_rows_for_wide_part = 1000000000;
CREATE TABLE t_rabitq_ref (id UInt64, v Array(Float32) CODEC(Quantized('rabitq', 8)))
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_rabitq SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(8)) FROM numbers(100);
ALTER TABLE t_rabitq UPDATE v = arrayMap(x -> -x, v) WHERE id % 3 = 0;
INSERT INTO t_rabitq_ref SELECT id, v FROM t_rabitq;

SELECT 'rabitq', part_type, has(substreams, 'v.quantized')
FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_rabitq' AND active AND column = 'v';
SELECT (SELECT sum(cityHash64(id, v.quantized)) FROM t_rabitq) = (SELECT sum(cityHash64(id, v.quantized)) FROM t_rabitq_ref);

-- A MATERIALIZED column: MATERIALIZE COLUMN.
CREATE TABLE t_materialized
(
    id UInt64,
    w Array(Float32),
    v Array(Float32) MATERIALIZED arrayMap(x -> x * 2, w) CODEC(Quantized('rabitq', 8))
)
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_materialized (id, w) SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(8)) FROM numbers(100);

ALTER TABLE t_materialized MATERIALIZE COLUMN v;
SELECT 'materialize column', has(substreams, 'v.quantized')
FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_materialized' AND active AND column = 'v';

SELECT count(), sum(length(v.quantized)) FROM t_materialized;

-- A MATERIALIZED column recomputed by an update of the column it is computed from.
CREATE TABLE t_dependency
(
    id UInt64,
    w Array(Float32),
    v Array(Float32) MATERIALIZED arrayMap(x -> x * 2, w) CODEC(Quantized('rabitq', 8))
)
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
CREATE TABLE t_dependency_ref (id UInt64, v Array(Float32) CODEC(Quantized('rabitq', 8)))
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_dependency (id, w) SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(8)) FROM numbers(100);
ALTER TABLE t_dependency UPDATE w = arrayMap(x -> -x, w) WHERE id % 3 = 0;
INSERT INTO t_dependency_ref SELECT id, v FROM t_dependency;

SELECT 'update dependency', has(substreams, 'v.quantized')
FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_dependency' AND active AND column = 'v';
SELECT countIf(v != arrayMap(x -> x * 2, w)) FROM t_dependency;
SELECT (SELECT sum(cityHash64(id, v.quantized)) FROM t_dependency) = (SELECT sum(cityHash64(id, v.quantized)) FROM t_dependency_ref);

-- The column-level block sizes apply only to the vectors of a mutated column too.
CREATE TABLE t_block_sizes (id UInt64, v Array(Float32) CODEC(Quantized('rabitq', 64)) SETTINGS (min_compress_block_size = 256))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, index_granularity = 64,
    index_granularity_bytes = '10Mi', min_compress_block_size = 65536, max_compress_block_size = 1048576,
    min_columns_to_activate_adaptive_write_buffer = 0, max_memory_ratio_to_activate_adaptive_write_buffer = 0;
INSERT INTO t_block_sizes SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(64)) FROM numbers(1024)
SETTINGS max_insert_threads = 1, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0;
OPTIMIZE TABLE t_block_sizes FINAL;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_block_sizes' AND active;
ALTER TABLE t_block_sizes UPDATE v = arrayMap(x -> -x, v) WHERE id % 3 = 0;

-- Compressed blocks the data granules start in: vectors, codes, granules.
SELECT uniqExact((`v.mark`).offset_in_compressed_file), uniqExact((`v.quantized.mark`).offset_in_compressed_file), count()
FROM mergeTreeIndex(currentDatabase(), t_block_sizes, with_marks = true) WHERE rows_in_granule > 0;

DROP TABLE t_product;
DROP TABLE t_rabitq;
DROP TABLE t_rabitq_ref;
DROP TABLE t_materialized;
DROP TABLE t_dependency;
DROP TABLE t_dependency_ref;
DROP TABLE t_block_sizes;
