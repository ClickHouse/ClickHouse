-- Tags: no-random-merge-tree-settings
-- ^ asserts exact granule counts, so the data layout must be deterministic.

-- Skip indexes on Tuple subcolumns must be used for every equivalent field-access form
-- (named subcolumn p.s, tupleElement(p, 's'), tupleElement(p, 1), p.1) even when the full
-- tuple column is also read (SELECT *). See https://github.com/ClickHouse/ClickHouse/issues/110040

-- The rewrite is done by FunctionToSubcolumnsPass (new analyzer only) under
-- optimize_functions_to_subcolumns; a single part keeps the granule count stable.
SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;
SET max_insert_threads = 1;

DROP TABLE IF EXISTS t_tuple_subcol_index;

CREATE TABLE t_tuple_subcol_index
(
    id UInt64,
    p Tuple(s String, n UInt64),
    INDEX idx_s p.s TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1,
    INDEX idx_n p.n TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4;

INSERT INTO t_tuple_subcol_index SELECT number, (toString(number), number) FROM numbers(64);

-- text index on p.s, SELECT * : all four access forms must prune to a single granule.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT * FROM t_tuple_subcol_index WHERE p.s = '10') WHERE explain ILIKE '%Granules: 1/16%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT * FROM t_tuple_subcol_index WHERE tupleElement(p, 's') = '10') WHERE explain ILIKE '%Granules: 1/16%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT * FROM t_tuple_subcol_index WHERE tupleElement(p, 1) = '10') WHERE explain ILIKE '%Granules: 1/16%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT * FROM t_tuple_subcol_index WHERE p.1 = '10') WHERE explain ILIKE '%Granules: 1/16%';

-- minmax index on p.n, SELECT * : same for the second field.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT * FROM t_tuple_subcol_index WHERE p.n = 10) WHERE explain ILIKE '%Granules: 1/16%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT * FROM t_tuple_subcol_index WHERE tupleElement(p, 'n') = 10) WHERE explain ILIKE '%Granules: 1/16%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT * FROM t_tuple_subcol_index WHERE tupleElement(p, 2) = 10) WHERE explain ILIKE '%Granules: 1/16%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT * FROM t_tuple_subcol_index WHERE p.2 = 10) WHERE explain ILIKE '%Granules: 1/16%';

-- narrow select list must keep pruning (unchanged behavior).
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT id FROM t_tuple_subcol_index WHERE tupleElement(p, 1) = '10') WHERE explain ILIKE '%Granules: 1/16%';

-- results are identical across all forms.
SELECT id FROM t_tuple_subcol_index WHERE p.s = '10';
SELECT id FROM t_tuple_subcol_index WHERE tupleElement(p, 's') = '10';
SELECT id FROM t_tuple_subcol_index WHERE tupleElement(p, 1) = '10';
SELECT id FROM t_tuple_subcol_index WHERE p.1 = '10';

DROP TABLE t_tuple_subcol_index;
