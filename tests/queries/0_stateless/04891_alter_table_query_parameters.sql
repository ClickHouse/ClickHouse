-- Query parameters are supported in the bare-name positions of `ALTER TABLE`
-- (`RENAME COLUMN`, `COMMENT COLUMN`, `ADD COLUMN ... AFTER`, `CLEAR COLUMN`,
-- `DROP COLUMN`, `DROP INDEX`, `DROP CONSTRAINT`), which are parsed by
-- `ParserCompoundIdentifier` with `allow_query_parameter_` enabled.

DROP TABLE IF EXISTS t_param_ddl;
CREATE TABLE t_param_ddl
(
    a UInt64,
    b UInt64,
    INDEX idx_b b TYPE minmax GRANULARITY 1,
    CONSTRAINT c_b CHECK b > 0
)
ENGINE = MergeTree
ORDER BY tuple();

-- `RENAME COLUMN`: parameterized source and target names.
SET param_from = 'a';
SET param_to = 'c';
ALTER TABLE t_param_ddl RENAME COLUMN {from:Identifier} TO {to:Identifier};
SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 't_param_ddl' AND name = {to:String} FORMAT TSV;

-- `COMMENT COLUMN`: parameterized column name.
SET param_col = 'c';
ALTER TABLE t_param_ddl COMMENT COLUMN {col:Identifier} 'renamed column';
SELECT name, comment FROM system.columns WHERE database = currentDatabase() AND table = 't_param_ddl' AND name = {col:String} FORMAT TSV;

-- `ADD COLUMN ... AFTER`: parameterized `AFTER` name.
SET param_after = 'c';
ALTER TABLE t_param_ddl ADD COLUMN d UInt64 AFTER {after:Identifier};
SELECT name, position FROM system.columns WHERE database = currentDatabase() AND table = 't_param_ddl' ORDER BY position FORMAT TSV;

-- `DROP INDEX`: parameterized index name.
SET param_idx = 'idx_b';
ALTER TABLE t_param_ddl DROP INDEX {idx:Identifier};
SELECT count() FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_param_ddl' FORMAT TSV;

-- `CLEAR COLUMN`: parameterized column name.
SET param_clear = 'd';
ALTER TABLE t_param_ddl CLEAR COLUMN {clear:Identifier};

-- `DROP COLUMN`: parameterized column name.
ALTER TABLE t_param_ddl DROP COLUMN {col:Identifier};
SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = 't_param_ddl' AND name = {col:String} FORMAT TSV;

-- `DROP CONSTRAINT`: parameterized constraint name.
SET param_constraint = 'c_b';
ALTER TABLE t_param_ddl DROP CONSTRAINT {constraint:Identifier};
SELECT count() FROM system.constraints WHERE database = currentDatabase() AND table = 't_param_ddl' FORMAT TSV;

DROP TABLE t_param_ddl;
