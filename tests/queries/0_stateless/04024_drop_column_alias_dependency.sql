-- Test that we cannot drop a column that an ALIAS column's expression depends on.
-- Dropping the ALIAS column itself is allowed; then the base column can be dropped.

SET enable_analyzer=1;

DROP TABLE IF EXISTS test_drop_column_alias_dep;

CREATE TABLE test_drop_column_alias_dep
(
    `data` JSON,
    `seq` UInt64,
    `item` String,
    `derived_hashes` Array(FixedString(32)) ALIAS arrayMap(item -> unhex(CAST(item.hash, 'String')), CAST(data.nested.entries, 'Array(JSON)'))
)
ENGINE = MergeTree
ORDER BY seq;

-- Dropping the column "item" should be allowed, since the ALIAS expression
-- only depends on "data" and "item" is a lambda parameter, not a real column.
ALTER TABLE test_drop_column_alias_dep DROP COLUMN item;

-- Dropping the column that the ALIAS depends on must fail.
ALTER TABLE test_drop_column_alias_dep DROP COLUMN data; -- { serverError ILLEGAL_COLUMN }

-- Dropping the ALIAS column is allowed.
ALTER TABLE test_drop_column_alias_dep DROP COLUMN derived_hashes;

-- Now dropping the base column is allowed.
ALTER TABLE test_drop_column_alias_dep DROP COLUMN data;

DROP TABLE IF EXISTS test_drop_column_alias_dep;


DROP TABLE IF EXISTS t;
CREATE TABLE t (a Int32, b Int32 ALIAS a + 1, c Int32 ALIAS b * 2) ENGINE = MergeTree;

ALTER TABLE t DROP COLUMN b; -- { serverError ILLEGAL_COLUMN }

DROP TABLE t;
