-- Tags: no-parallel-replicas
-- (the two-stage codes rewrite is deliberately disabled under parallel replicas, so the plan-shape assertions below
--  cannot hold there.)
-- The `Quantized(...)` codec on `Array(Int8)` and `Array(UInt8)` columns. Methods `int8` and `prefix` with format `int8`
-- are rejected for them. For `rabitq` and `turboquant`, the elements of an `Array(UInt8)` column and of the reference
-- vector are shifted by -128 before encoding.

SET enable_quantized_codec = 1;
SET vector_search_use_quantized_codes = 1;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 1000000;

DROP TABLE IF EXISTS qc_i8;
DROP TABLE IF EXISTS qc_u8;
DROP TABLE IF EXISTS qc_f32_same;
DROP TABLE IF EXISTS qc_f32_shifted;
DROP TABLE IF EXISTS qc_tmp;

-- Accepted methods.
CREATE TABLE qc_tmp (vec Array(Int8) CODEC(Quantized('turboquant', 64))) ENGINE = MergeTree ORDER BY tuple();
DROP TABLE qc_tmp;
CREATE TABLE qc_tmp (vec Array(UInt8) CODEC(Quantized('product', 64, 8, 8))) ENGINE = MergeTree ORDER BY tuple();
DROP TABLE qc_tmp;
CREATE TABLE qc_tmp (vec Array(UInt8) CODEC(Quantized('prefix', 64, 16, 'bf16'))) ENGINE = MergeTree ORDER BY tuple();
DROP TABLE qc_tmp;

-- Rejected methods and element types.
CREATE TABLE qc_tmp (vec Array(Int8) CODEC(Quantized('int8', 64))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError ILLEGAL_COLUMN }
CREATE TABLE qc_tmp (vec Array(UInt8) CODEC(Quantized('int8', 64))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError ILLEGAL_COLUMN }
CREATE TABLE qc_tmp (vec Array(UInt8) CODEC(Quantized('prefix', 64, 16, 'int8'))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError ILLEGAL_COLUMN }
CREATE TABLE qc_tmp (vec Array(UInt32) CODEC(Quantized('rabitq', 64))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError ILLEGAL_COLUMN }

CREATE TABLE qc_i8 (id UInt32, vec Array(Int8) CODEC(Quantized('rabitq', 64))) ENGINE = MergeTree ORDER BY id;
CREATE TABLE qc_u8 (id UInt32, vec Array(UInt8) CODEC(Quantized('rabitq', 64))) ENGINE = MergeTree ORDER BY id;
CREATE TABLE qc_f32_same (id UInt32, vec Array(Float32) CODEC(Quantized('rabitq', 64))) ENGINE = MergeTree ORDER BY id;
CREATE TABLE qc_f32_shifted (id UInt32, vec Array(Float32) CODEC(Quantized('rabitq', 64))) ENGINE = MergeTree ORDER BY id;

-- Off-center unsigned data: all elements are in [80, 199].
INSERT INTO qc_u8 SELECT number, arrayMap(j -> toUInt8(80 + sipHash64(number, j) % 120), range(64)) FROM numbers(4000);
INSERT INTO qc_i8 SELECT number, arrayMap(j -> toInt8(toInt16(sipHash64(number, j) % 200) - 100), range(64)) FROM numbers(4000);
INSERT INTO qc_f32_same SELECT id, arrayMap(x -> toFloat32(x), vec) FROM qc_i8;
INSERT INTO qc_f32_shifted SELECT id, arrayMap(x -> toFloat32(x) - 128, vec) FROM qc_u8;

SELECT 'code_length', length(vec.quantized) FROM qc_u8 GROUP BY length(vec.quantized);

-- `Int8` elements are encoded as they are, `UInt8` elements are encoded after the shift.
SELECT 'int8_codes_equal_float',
    (SELECT groupArray(vec.quantized) FROM (SELECT vec.quantized FROM qc_i8 ORDER BY id))
    = (SELECT groupArray(vec.quantized) FROM (SELECT vec.quantized FROM qc_f32_same ORDER BY id));
SELECT 'uint8_codes_equal_shifted_float',
    (SELECT groupArray(vec.quantized) FROM (SELECT vec.quantized FROM qc_u8 ORDER BY id))
    = (SELECT groupArray(vec.quantized) FROM (SELECT vec.quantized FROM qc_f32_shifted ORDER BY id));

-- The rewrite applies with integer reference vectors, for both distance functions.
SELECT 'plan_uint8_cosine', countIf(explain ILIKE '%quantized shortlist%') > 0
FROM (EXPLAIN PLAN SELECT id FROM qc_u8 ORDER BY cosineDistance(vec, (SELECT vec FROM qc_u8 WHERE id = 123)) LIMIT 5);
SELECT 'plan_uint8_l2', countIf(explain ILIKE '%quantized shortlist%') > 0
FROM (EXPLAIN PLAN SELECT id FROM qc_u8 ORDER BY L2Distance(vec, (SELECT vec FROM qc_u8 WHERE id = 123)) LIMIT 5);
SELECT 'plan_int8_l2', countIf(explain ILIKE '%quantized shortlist%') > 0
FROM (EXPLAIN PLAN SELECT id FROM qc_i8 ORDER BY L2Distance(vec, (SELECT vec FROM qc_i8 WHERE id = 123)) LIMIT 5);
SELECT 'plan_uint16_literal', countIf(explain ILIKE '%quantized shortlist%') > 0
FROM (EXPLAIN PLAN SELECT id FROM qc_u8 ORDER BY L2Distance(vec, arrayMap(x -> toUInt16(x) * 4, range(64))) LIMIT 5);

-- With a shortlist covering all rows, the codes path reproduces the exact top-k.
WITH (SELECT vec FROM qc_u8 WHERE id = 123) AS ref
SELECT 'uint8_cosine_exact',
    (SELECT groupArray(id) FROM (SELECT id, cosineDistance(vec, ref) AS d FROM qc_u8 ORDER BY d, id LIMIT 10 SETTINGS vector_search_use_quantized_codes = 0))
    = (SELECT groupArray(id) FROM (SELECT id FROM qc_u8 ORDER BY cosineDistance(vec, ref) LIMIT 10 SETTINGS vector_search_index_fetch_multiplier = 1000));
WITH (SELECT vec FROM qc_u8 WHERE id = 123) AS ref
SELECT 'uint8_l2_exact',
    (SELECT groupArray(id) FROM (SELECT id, L2Distance(vec, ref) AS d FROM qc_u8 ORDER BY d, id LIMIT 10 SETTINGS vector_search_use_quantized_codes = 0))
    = (SELECT groupArray(id) FROM (SELECT id FROM qc_u8 ORDER BY L2Distance(vec, ref) LIMIT 10 SETTINGS vector_search_index_fetch_multiplier = 1000));
WITH (SELECT vec FROM qc_i8 WHERE id = 123) AS ref
SELECT 'int8_l2_exact',
    (SELECT groupArray(id) FROM (SELECT id, L2Distance(vec, ref) AS d FROM qc_i8 ORDER BY d, id LIMIT 10 SETTINGS vector_search_use_quantized_codes = 0))
    = (SELECT groupArray(id) FROM (SELECT id FROM qc_i8 ORDER BY L2Distance(vec, ref) LIMIT 10 SETTINGS vector_search_index_fetch_multiplier = 1000));
WITH arrayMap(x -> toUInt16(x) * 4, range(64)) AS ref
SELECT 'uint16_literal_exact',
    (SELECT groupArray(id) FROM (SELECT id, L2Distance(vec, ref) AS d FROM qc_u8 ORDER BY d, id LIMIT 10 SETTINGS vector_search_use_quantized_codes = 0))
    = (SELECT groupArray(id) FROM (SELECT id FROM qc_u8 ORDER BY L2Distance(vec, ref) LIMIT 10 SETTINGS vector_search_index_fetch_multiplier = 1000));

-- The query vector is returned first.
SELECT 'nearest_is_self',
    (SELECT id FROM qc_u8 ORDER BY L2Distance(vec, (SELECT vec FROM qc_u8 WHERE id = 123)) LIMIT 1 SETTINGS vector_search_index_fetch_multiplier = 100) = 123;

DROP TABLE qc_i8;
DROP TABLE qc_u8;
DROP TABLE qc_f32_same;
DROP TABLE qc_f32_shifted;
