-- Tags: no-random-merge-tree-settings
-- ^ asserts exact granule counts, so the data layout must be deterministic.

-- QBit subcolumns are accessed via tupleElement (vec.N is sugar for tupleElement(vec, N)).
-- Under SELECT * the full QBit column is also read, so without whitelisting the
-- tupleElement transformer for the filter, the WHERE predicate does not rewrite to
-- the vec.N subcolumn read. Sibling of the Tuple and Variant cases in
-- 04401_skip_index_on_tuple_subcolumn_with_tuple_element /
-- 04402_skip_index_on_variant_subcolumn_with_variant_element; see
-- https://github.com/ClickHouse/ClickHouse/issues/110040

-- The rewrite is done by FunctionToSubcolumnsPass (new analyzer only) under
-- optimize_functions_to_subcolumns; a single part keeps the granule count stable.
SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;
SET max_insert_threads = 1;

DROP TABLE IF EXISTS t_qbit_subcol;

CREATE TABLE t_qbit_subcol
(
    id UInt64,
    vec QBit(Float32, 4)
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4;

INSERT INTO t_qbit_subcol SELECT number, arrayMap(x -> toFloat32(number + x), range(4))::QBit(Float32, 4) FROM numbers(64);

-- Under SELECT * the WHERE predicate on the QBit subcolumn must rewrite to the vec.1
-- subcolumn read, for both equivalent access forms (vec.1, tupleElement(vec, 1)).
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM t_qbit_subcol WHERE tupleElement(vec, 1) = CAST(unhex('00'), 'FixedString(1)')) WHERE explain ILIKE '%column_name: vec.1%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM t_qbit_subcol WHERE vec.1 = CAST(unhex('00'), 'FixedString(1)')) WHERE explain ILIKE '%column_name: vec.1%';

-- Narrow select list keeps rewriting (unchanged behavior).
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT id FROM t_qbit_subcol WHERE tupleElement(vec, 1) = CAST(unhex('00'), 'FixedString(1)')) WHERE explain ILIKE '%column_name: vec.1%';

-- A skip index on a QBit subcolumn prunes granules under SELECT *. vec.8 is the first
-- bit plane that varies across granules for this data, so a set index on it is selective.
DROP TABLE IF EXISTS t_qbit_subcol_index;

CREATE TABLE t_qbit_subcol_index
(
    id UInt64,
    vec QBit(Float32, 4),
    INDEX idx_8 vec.8 TYPE set(100) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4;

INSERT INTO t_qbit_subcol_index SELECT number, arrayMap(x -> toFloat32(number + x), range(4))::QBit(Float32, 4) FROM numbers(64);

SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT * FROM t_qbit_subcol_index WHERE tupleElement(vec, 8) = CAST(unhex('02'), 'FixedString(1)')) WHERE explain ILIKE '%Granules: 1/16%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT * FROM t_qbit_subcol_index WHERE vec.8 = CAST(unhex('02'), 'FixedString(1)')) WHERE explain ILIKE '%Granules: 1/16%';

-- Results are identical across both access forms.
SELECT id FROM t_qbit_subcol_index WHERE vec.8 = CAST(unhex('02'), 'FixedString(1)') ORDER BY id;
SELECT id FROM t_qbit_subcol_index WHERE tupleElement(vec, 8) = CAST(unhex('02'), 'FixedString(1)') ORDER BY id;

DROP TABLE t_qbit_subcol_index;
DROP TABLE t_qbit_subcol;
