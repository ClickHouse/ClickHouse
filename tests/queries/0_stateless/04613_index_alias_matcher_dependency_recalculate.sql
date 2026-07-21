-- A skip index that references an ALIAS column must be rebuilt when the alias body
-- changes, even when the dependency is only visible after column matcher expansion
-- inside another alias body.

-- Matchers do not match ALIAS columns unless this setting is enabled.
SET asterisk_include_alias_columns = 1;

DROP TABLE IF EXISTS t_index_alias_matcher_dep;

CREATE TABLE t_index_alias_matcher_dep
(
    a UInt64,
    x UInt64 ALIAS a + 1,
    y UInt64 ALIAS plus(COLUMNS('^x$'), 0),
    INDEX idx y TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY a;

INSERT INTO t_index_alias_matcher_dep SELECT number FROM numbers(1000);

-- The dependency of `idx` on `x` is hidden inside `COLUMNS('^x$')` in the body of `y`.
-- The index over the pre-existing part must be rebuilt from the new alias body,
-- otherwise the stale range [1, 1000] would prune `y = 1999` incorrectly.
ALTER TABLE t_index_alias_matcher_dep MODIFY COLUMN x UInt64 ALIAS a + 1000;

SELECT count() FROM t_index_alias_matcher_dep WHERE y = 1999 SETTINGS force_data_skipping_indices = 'idx';

-- THROW mode must also see the matcher-hidden dependency and reject the alter.
ALTER TABLE t_index_alias_matcher_dep MODIFY SETTING alter_column_secondary_index_mode = 'throw';
ALTER TABLE t_index_alias_matcher_dep MODIFY COLUMN x UInt64 ALIAS a + 2000; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

DROP TABLE t_index_alias_matcher_dep;

-- An index dropped by the same ALTER leaves no stale index files behind, so it must
-- not be reported as affected: THROW mode must not reject the alter, and REBUILD
-- mode must not queue a rebuild for the removed index.

DROP TABLE IF EXISTS t_index_alias_drop_same_alter;

CREATE TABLE t_index_alias_drop_same_alter
(
    a UInt64,
    x UInt64 ALIAS a + 1,
    INDEX idx x TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY a
SETTINGS alter_column_secondary_index_mode = 'throw';

INSERT INTO t_index_alias_drop_same_alter SELECT number FROM numbers(1000);

-- Without the DROP INDEX, this alter would be rejected in THROW mode.
ALTER TABLE t_index_alias_drop_same_alter
    DROP INDEX idx,
    MODIFY COLUMN x UInt64 ALIAS a + 2;

SELECT count() FROM t_index_alias_drop_same_alter WHERE x = 1001;
SHOW CREATE TABLE t_index_alias_drop_same_alter;

DROP TABLE t_index_alias_drop_same_alter;
