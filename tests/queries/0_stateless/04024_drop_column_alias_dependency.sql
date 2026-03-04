-- Test that we cannot drop a column that an ALIAS column's expression depends on.
-- Dropping the ALIAS column itself is allowed; then the base column can be dropped.

DROP TABLE IF EXISTS test_drop_column_alias_dep;

CREATE TABLE test_drop_column_alias_dep
(
    `data` JSON,
    `seq` UInt64,
    `derived_hashes` Array(FixedString(32)) ALIAS arrayMap(item -> unhex(CAST(item.hash, 'String')), CAST(data.nested.entries, 'Array(JSON)'))
)
ENGINE = MergeTree
ORDER BY seq;

-- Dropping the column that the ALIAS depends on must fail.
ALTER TABLE test_drop_column_alias_dep DROP COLUMN data; -- { serverError ILLEGAL_COLUMN }

-- Dropping the ALIAS column is allowed.
ALTER TABLE test_drop_column_alias_dep DROP COLUMN derived_hashes;

-- Now dropping the base column is allowed.
ALTER TABLE test_drop_column_alias_dep DROP COLUMN data;

DROP TABLE IF EXISTS test_drop_column_alias_dep;
