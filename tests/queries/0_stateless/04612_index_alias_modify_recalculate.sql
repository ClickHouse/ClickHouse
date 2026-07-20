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

-- Rows inserted before the alias change: the part's index files are built from `a + 1`, range [1, 1000].
INSERT INTO t_index_alias_realias SELECT number FROM numbers(1000);

ALTER TABLE t_index_alias_realias MODIFY COLUMN x UInt64 ALIAS a + 2;

-- After changing the alias body, the index expression follows the new definition.
SELECT expr FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_index_alias_realias';
SHOW CREATE TABLE t_index_alias_realias;

-- The index over the pre-existing part was rebuilt from the new alias body to the range [2, 1001].
-- With the stale index files, `x = 1001` would be pruned incorrectly and return 0.
SELECT count() FROM t_index_alias_realias WHERE x = 1001 SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM t_index_alias_realias WHERE x = 500 SETTINGS force_data_skipping_indices = 'idx';

-- Removing the alias converts the column to a physical one that reads as the type default
-- from old parts; the index is rebuilt accordingly instead of keeping the stale range.
ALTER TABLE t_index_alias_realias MODIFY COLUMN x REMOVE ALIAS;
SELECT count() FROM t_index_alias_realias WHERE x = 0 SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE t_index_alias_realias;

-- An alias body change must also rebuild indices that reference the alias transitively.

DROP TABLE IF EXISTS t_index_alias_chain;

CREATE TABLE t_index_alias_chain
(
    a UInt64,
    x UInt64 ALIAS a + 1,
    y UInt64 ALIAS x * 2,
    INDEX idx y TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY a;

INSERT INTO t_index_alias_chain SELECT number FROM numbers(1000);

ALTER TABLE t_index_alias_chain MODIFY COLUMN x UInt64 ALIAS a + 1000;

-- y = (999 + 1000) * 2 lies outside the stale range [2, 2000] and would be pruned incorrectly.
SELECT count() FROM t_index_alias_chain WHERE y = 3998 SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE t_index_alias_chain;

-- The same rebuild happens for replicated tables through the replication log.

DROP TABLE IF EXISTS t_index_alias_replicated;

CREATE TABLE t_index_alias_replicated
(
    a UInt64,
    x UInt64 ALIAS a + 1,
    INDEX idx x TYPE minmax GRANULARITY 1
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04612_index_alias', 'r1') ORDER BY a;

INSERT INTO t_index_alias_replicated SELECT number FROM numbers(1000);

ALTER TABLE t_index_alias_replicated MODIFY COLUMN x UInt64 ALIAS a + 2 SETTINGS alter_sync = 2, mutations_sync = 2;

SELECT count() FROM t_index_alias_replicated WHERE x = 1001 SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE t_index_alias_replicated;

-- With `alter_column_secondary_index_mode = 'throw'`, changing an alias used by an index is forbidden.

DROP TABLE IF EXISTS t_index_alias_throw;

CREATE TABLE t_index_alias_throw
(
    a UInt64,
    x UInt64 ALIAS a + 1,
    INDEX idx x TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY a SETTINGS alter_column_secondary_index_mode = 'throw';

ALTER TABLE t_index_alias_throw MODIFY COLUMN x UInt64 ALIAS a + 2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- Modifying an alias no index depends on is still allowed.
ALTER TABLE t_index_alias_throw ADD COLUMN z UInt64 ALIAS a + 3;
ALTER TABLE t_index_alias_throw MODIFY COLUMN z UInt64 ALIAS a + 4;

DROP TABLE t_index_alias_throw;

-- With `alter_column_secondary_index_mode = 'drop'`, the stale index files are cleared
-- instead of rebuilt, so the query reads all granules and stays correct.

DROP TABLE IF EXISTS t_index_alias_drop;

CREATE TABLE t_index_alias_drop
(
    a UInt64,
    x UInt64 ALIAS a + 1,
    INDEX idx x TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY a SETTINGS alter_column_secondary_index_mode = 'drop';

INSERT INTO t_index_alias_drop SELECT number FROM numbers(1000);

ALTER TABLE t_index_alias_drop MODIFY COLUMN x UInt64 ALIAS a + 2;

SELECT count() FROM t_index_alias_drop WHERE x = 1001;

DROP TABLE t_index_alias_drop;

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
