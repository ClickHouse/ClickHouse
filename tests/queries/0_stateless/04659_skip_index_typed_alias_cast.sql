-- A typed ALIAS in a skip index expression must index the alias value (the semantic
-- `_CAST` applied), not the raw source expression: for narrowing alias types the two
-- domains differ, and query analysis substitutes the cast form for predicates over
-- the alias.

SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 0;
SET materialize_skip_indexes_on_insert = 1;

DROP TABLE IF EXISTS t_index_typed_alias;

CREATE TABLE t_index_typed_alias
(
    a UInt16,
    b UInt8 ALIAS a,
    INDEX idx b TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1;

-- 300 truncates to 44 in UInt8; 100 stays 100.
INSERT INTO t_index_typed_alias VALUES (300), (100);

-- The index must be usable for predicates over the alias and track the alias value.
SELECT count() FROM t_index_typed_alias WHERE b = 44 SETTINGS force_data_skipping_indices = 'idx', enable_analyzer = 1;
SELECT count() FROM t_index_typed_alias WHERE b = 44 SETTINGS force_data_skipping_indices = 'idx', enable_analyzer = 0;

-- No row has alias value 300: the raw source domain must not leak into the index.
SELECT count() FROM t_index_typed_alias WHERE b = 300 SETTINGS force_data_skipping_indices = 'idx', enable_analyzer = 1;
SELECT count() FROM t_index_typed_alias WHERE b = 300 SETTINGS force_data_skipping_indices = 'idx', enable_analyzer = 0;

-- The index actually prunes: only the granule holding alias value 44 is read.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_index_typed_alias WHERE b = 44 SETTINGS enable_analyzer = 1) WHERE explain LIKE '%Granules:%' SETTINGS enable_analyzer = 1;

-- The persisted definition keeps the live alias reference, while the analyzed
-- expression carries the alias type conversion.
SELECT extract(create_table_query, 'INDEX idx .* GRANULARITY 1') FROM system.tables WHERE database = currentDatabase() AND name = 't_index_typed_alias';
SELECT expr FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_index_typed_alias' AND name = 'idx';

DROP TABLE t_index_typed_alias;
