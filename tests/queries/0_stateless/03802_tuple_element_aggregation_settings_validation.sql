-- Contract of `allow_tuple_element_aggregation`: immutable after creation;
-- when enabled, plain Tuple sorting key is rejected on CREATE and ATTACH;
-- silently ignored by engines that don't support it.

-- Immutability.
CREATE TABLE test_alter_table (`n` Tuple(a UInt32, b UInt32)) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE test_alter_table MODIFY SETTING allow_tuple_element_aggregation = 1; -- { serverError READONLY_SETTING }
DROP TABLE test_alter_table;

-- CREATE path.
CREATE TABLE test_tuple_order (`n` Tuple(a UInt32, b UInt32)) ENGINE = SummingMergeTree ORDER BY n SETTINGS allow_tuple_element_aggregation = 1; -- { serverError BAD_ARGUMENTS }

-- ATTACH path: validation must also reject plain Tuple sorting key on ATTACH.
ATTACH TABLE test_attach_reject UUID 'ffffffff-ffff-ffff-ffff-ffffffffffff' (`n` Tuple(a UInt32, b UInt32)) ENGINE = SummingMergeTree ORDER BY n SETTINGS allow_tuple_element_aggregation = 1; -- { serverError BAD_ARGUMENTS }

-- Unsupported engines accept the setting silently.
CREATE TABLE test_replacing_engine (`n` Tuple(a UInt32, b UInt32)) ENGINE = ReplacingMergeTree ORDER BY tuple() SETTINGS allow_tuple_element_aggregation = 1;
DROP TABLE test_replacing_engine;

CREATE TABLE test_collapsing_engine (`n` Tuple(a UInt32, b UInt32), `sign` Int8) ENGINE = CollapsingMergeTree(sign) ORDER BY tuple() SETTINGS allow_tuple_element_aggregation = 1;
DROP TABLE test_collapsing_engine;

CREATE TABLE test_normal_engine (`n` Tuple(a UInt32, b UInt32)) ENGINE = MergeTree ORDER BY tuple() SETTINGS allow_tuple_element_aggregation = 1;
DROP TABLE test_normal_engine;