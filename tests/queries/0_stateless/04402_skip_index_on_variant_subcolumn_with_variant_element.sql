-- Tags: no-random-merge-tree-settings
-- ^ asserts exact granule counts, so the data layout must be deterministic.

-- Skip indexes on Variant subcolumns must be used for both equivalent field-access forms
-- (named subcolumn v.String, variantElement(v, 'String')) even when the full Variant column
-- is also read (SELECT *). Sibling of the Tuple case in
-- 04401_skip_index_on_tuple_subcolumn_with_tuple_element; see
-- https://github.com/ClickHouse/ClickHouse/issues/110040

-- The rewrite is done by FunctionToSubcolumnsPass (new analyzer only) under
-- optimize_functions_to_subcolumns; a single part keeps the granule count stable.
SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;
SET max_insert_threads = 1;
SET allow_experimental_variant_type = 1;
SET allow_suspicious_variant_types = 1;

DROP TABLE IF EXISTS t_variant_subcol_index;

CREATE TABLE t_variant_subcol_index
(
    id UInt64,
    v Variant(String, UInt64),
    INDEX idx_s v.String TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1,
    INDEX idx_u v.UInt64 TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4;

-- All rows use the String discriminant so the text index is fully populated.
INSERT INTO t_variant_subcol_index SELECT number, ('s' || toString(number))::Variant(String, UInt64) FROM numbers(64);

-- text index on v.String, SELECT * : both access forms must prune to a single granule.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT * FROM t_variant_subcol_index WHERE v.String = 's11') WHERE explain ILIKE '%Granules: 1/16%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT * FROM t_variant_subcol_index WHERE variantElement(v, 'String') = 's11') WHERE explain ILIKE '%Granules: 1/16%';

-- narrow select list must keep pruning (unchanged behavior).
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT id FROM t_variant_subcol_index WHERE variantElement(v, 'String') = 's11') WHERE explain ILIKE '%Granules: 1/16%';

-- results are identical across both forms.
SELECT id FROM t_variant_subcol_index WHERE v.String = 's11';
SELECT id FROM t_variant_subcol_index WHERE variantElement(v, 'String') = 's11';

DROP TABLE t_variant_subcol_index;
