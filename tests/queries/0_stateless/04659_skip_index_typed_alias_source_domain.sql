-- A typed ALIAS in a skip index expression is indexed through its bare body, without a
-- conversion to the declared alias type: skip index files are addressed by index name and
-- carry no type information, so converting here would reinterpret the index files of parts
-- written earlier - including by server versions that predate matcher expansion in index
-- expressions, which accept such an index too - as the narrower type and prune granules
-- that do match. Indexing the source expression can only lose pruning, never correctness.

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

-- A `set` index stores the indexed values and re-applies the predicate to them, so it
-- prunes on predicates over the alias even though it is built over the source domain.
SELECT count() FROM t_index_typed_alias WHERE b = 44 SETTINGS force_data_skipping_indices = 'idx', enable_analyzer = 1;
SELECT count() FROM t_index_typed_alias WHERE b = 44 SETTINGS force_data_skipping_indices = 'idx', enable_analyzer = 0;

-- No row has alias value 300, and the index must not report a false positive for it.
SELECT count() FROM t_index_typed_alias WHERE b = 300 SETTINGS force_data_skipping_indices = 'idx', enable_analyzer = 1;
SELECT count() FROM t_index_typed_alias WHERE b = 300 SETTINGS force_data_skipping_indices = 'idx', enable_analyzer = 0;

-- The index actually prunes: only the granule holding alias value 44 is read.
-- The leading indentation is stripped along with the tree-drawing characters that appear
-- when the plan is nested deeper, as it is with parallel replicas.
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_index_typed_alias WHERE b = 44 SETTINGS enable_analyzer = 1) WHERE explain LIKE '%Granules:%' SETTINGS enable_analyzer = 1;

-- The persisted definition keeps the live alias reference, and the analyzed expression is
-- the alias body itself - no `_CAST` to `UInt8` - which is what the index files hold.
SELECT extract(create_table_query, 'INDEX idx .* GRANULARITY 1') FROM system.tables WHERE database = currentDatabase() AND name = 't_index_typed_alias';
SELECT expr FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_index_typed_alias' AND name = 'idx';

DROP TABLE t_index_typed_alias;

-- A `minmax` index compares the predicate against stored bounds, so over the source domain
-- it cannot serve predicates on the narrowed alias - but it must never drop matching rows.
DROP TABLE IF EXISTS t_index_typed_alias_minmax;

CREATE TABLE t_index_typed_alias_minmax
(
    a UInt16,
    b UInt8 ALIAS a,
    INDEX idx b TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO t_index_typed_alias_minmax VALUES (300), (100);

SELECT count() FROM t_index_typed_alias_minmax WHERE b = 44 SETTINGS enable_analyzer = 1;
SELECT count() FROM t_index_typed_alias_minmax WHERE b = 44 SETTINGS enable_analyzer = 0;
SELECT count() FROM t_index_typed_alias_minmax WHERE b = 100 SETTINGS enable_analyzer = 1;
SELECT count() FROM t_index_typed_alias_minmax WHERE b = 100 SETTINGS enable_analyzer = 0;
SELECT count() FROM t_index_typed_alias_minmax WHERE b = 300 SETTINGS enable_analyzer = 1;
SELECT count() FROM t_index_typed_alias_minmax WHERE b = 300 SETTINGS enable_analyzer = 0;

SELECT expr FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_index_typed_alias_minmax' AND name = 'idx';

DROP TABLE t_index_typed_alias_minmax;

-- A matcher that expands to the typed alias resolves to the same indexed expression.
DROP TABLE IF EXISTS t_index_typed_alias_matcher;

CREATE TABLE t_index_typed_alias_matcher
(
    a UInt16,
    b UInt8 ALIAS a,
    INDEX idx COLUMNS('^b$') TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO t_index_typed_alias_matcher VALUES (300), (100);

SELECT count() FROM t_index_typed_alias_matcher WHERE b = 44 SETTINGS force_data_skipping_indices = 'idx', enable_analyzer = 1;
SELECT expr FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_index_typed_alias_matcher' AND name = 'idx';

DROP TABLE t_index_typed_alias_matcher;
