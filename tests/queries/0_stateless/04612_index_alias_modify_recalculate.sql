-- A skip index declared on an ALIAS column must keep the alias reference in its
-- persisted definition, so that `ALTER TABLE ... MODIFY COLUMN ... ALIAS` rebuilds
-- the index expression from the new alias body.

DROP TABLE IF EXISTS t_index_alias_realias;

CREATE TABLE t_index_alias_realias
(
    a UInt64,
    x UInt64 ALIAS a + 1,
    INDEX idx x TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY a;

-- The persisted definition keeps the alias reference, while the analyzed expression uses the alias body.
SELECT expr FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_index_alias_realias';
SHOW CREATE TABLE t_index_alias_realias;

ALTER TABLE t_index_alias_realias MODIFY COLUMN x UInt64 ALIAS a + 2;

-- After changing the alias body, the index expression follows the new definition.
SELECT expr FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_index_alias_realias';
SHOW CREATE TABLE t_index_alias_realias;

INSERT INTO t_index_alias_realias SELECT number FROM numbers(1000);

-- The index is usable for queries on the alias column after the alias change.
SELECT count() FROM t_index_alias_realias WHERE x = 500 SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE t_index_alias_realias;

-- Matcher expansion in an index definition is still frozen at creation time:
-- adding a column matching the pattern afterwards must not change the index.

DROP TABLE IF EXISTS t_index_matcher_frozen;

CREATE TABLE t_index_matcher_frozen
(
    a1 UInt64,
    a2 UInt64,
    INDEX idxm COLUMNS('^a') TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY a1;

SHOW CREATE TABLE t_index_matcher_frozen;

ALTER TABLE t_index_matcher_frozen ADD COLUMN a3 UInt64;

SHOW CREATE TABLE t_index_matcher_frozen;

DROP TABLE t_index_matcher_frozen;
