-- Tests that a bloom_filter skip index is not used for a condition built only from != or NOT IN.
-- Such a condition can never skip a granule, because a bloom filter can only tell that a value is
-- absent from a granule, not that every row of the granule holds it. Reading the index for it is
-- pure overhead, so the index is not selected and force_data_skipping_indices reports it as unused.
-- A negated condition combined with a positive one still uses the index.

DROP TABLE IF EXISTS t_bf_neg;

CREATE TABLE t_bf_neg
(
    id UInt64,
    v UInt64,
    w UInt64,
    arr Array(UInt64),
    INDEX idx_v v TYPE bloom_filter GRANULARITY 1,
    INDEX idx_w w TYPE bloom_filter GRANULARITY 1,
    INDEX idx_arr arr TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0;

INSERT INTO t_bf_neg SELECT number, number % 8, number % 8, [number % 8] FROM numbers(64);

SELECT '-- notEquals does not use the index';
SELECT count() FROM t_bf_neg WHERE v != 100 SETTINGS force_data_skipping_indices = 'idx_v'; -- { serverError INDEX_NOT_USED }

SELECT '-- NOT IN does not use the index';
SELECT count() FROM t_bf_neg WHERE v NOT IN (100, 101) SETTINGS force_data_skipping_indices = 'idx_v'; -- { serverError INDEX_NOT_USED }

SELECT '-- a negated condition next to a positive one still prunes';
SET explain_query_plan_default = 'legacy';
SELECT argMax(toUInt64(extract(explain, 'Granules: (\\d+)/')), rowNumberInAllBlocks())
     < argMax(toUInt64(extract(explain, 'Granules: \\d+/(\\d+)')), rowNumberInAllBlocks())
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_neg WHERE v = 3 AND v != 100)
WHERE explain LIKE '%Granules:%'
SETTINGS use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_bf_neg WHERE v = 3 AND v != 100 SETTINGS force_data_skipping_indices = 'idx_v';

SELECT '-- equals and IN still use the index';
SELECT count() FROM t_bf_neg WHERE v = 100 SETTINGS force_data_skipping_indices = 'idx_v';
SELECT count() FROM t_bf_neg WHERE v IN (100, 101) SETTINGS force_data_skipping_indices = 'idx_v';

SELECT '-- array conditions still use the index';
SELECT count() FROM t_bf_neg WHERE has(arr, 3) SETTINGS force_data_skipping_indices = 'idx_arr';
SELECT count() FROM t_bf_neg WHERE indexOf(arr, 3) = 1 SETTINGS force_data_skipping_indices = 'idx_arr';

SELECT '-- a disjunction over two columns still uses both indexes';
SELECT count() FROM t_bf_neg WHERE v = 3 OR w = 5 SETTINGS force_data_skipping_indices = 'idx_v';
SELECT count() FROM t_bf_neg WHERE v = 3 OR w = 5 SETTINGS force_data_skipping_indices = 'idx_w';

SELECT '-- a negated atom in a disjunction leaves only that index unused';
SELECT count() FROM t_bf_neg WHERE v != 100 OR w = 5 SETTINGS force_data_skipping_indices = 'idx_v'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM t_bf_neg WHERE v != 100 OR w = 5 SETTINGS force_data_skipping_indices = 'idx_w';
SELECT count() FROM t_bf_neg WHERE v != 100 OR w = 5;
SELECT count() FROM t_bf_neg WHERE v != 100 OR w = 5 SETTINGS use_skip_indexes = 0;

SELECT '-- a positive disjunction still prunes by combining both indexes';
SELECT argMax(toUInt64(extract(explain, 'Granules: (\\d+)/')), rowNumberInAllBlocks())
FROM (EXPLAIN indexes = 1 SELECT sum(id) FROM t_bf_neg WHERE v = 3 OR w = 100)
WHERE explain LIKE '%Granules:%'
SETTINGS use_skip_indexes_for_disjunctions = 1, use_skip_indexes_on_data_read = 0;

SELECT '-- a doubly negated condition folds back to a positive atom and still uses the index';
SELECT count() FROM t_bf_neg WHERE NOT (v != 100) SETTINGS force_data_skipping_indices = 'idx_v';
SELECT count() FROM t_bf_neg WHERE NOT (v NOT IN (100, 101)) SETTINGS force_data_skipping_indices = 'idx_v';

SELECT '-- results are unchanged';
SELECT count() FROM t_bf_neg WHERE v != 100;
SELECT count() FROM t_bf_neg WHERE v != 100 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_neg WHERE v NOT IN (100, 101);
SELECT count() FROM t_bf_neg WHERE v NOT IN (100, 101) SETTINGS use_skip_indexes = 0;

DROP TABLE t_bf_neg;
