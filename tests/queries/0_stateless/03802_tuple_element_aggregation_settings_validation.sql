-- Contract of `allow_tuple_element_aggregation`: immutable after creation;
-- when enabled, plain Tuple sorting key is rejected on CREATE and ATTACH;
-- silently ignored by engines that don't support it.

-- Immutability.
CREATE TABLE test_alter_table (`n` Tuple(a UInt32, b UInt32)) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE test_alter_table MODIFY SETTING allow_tuple_element_aggregation = 1; -- { serverError READONLY_SETTING }
DROP TABLE test_alter_table;

-- CREATE path.
CREATE TABLE test_tuple_order (`n` Tuple(a UInt32, b UInt32)) ENGINE = SummingMergeTree ORDER BY n SETTINGS allow_tuple_element_aggregation = 1; -- { serverError BAD_ARGUMENTS }

-- ATTACH path (UUID form needed by Atomic DB for ATTACH with full definition).
ATTACH TABLE test_attach_reject UUID '00000000-0000-0000-0000-000000038020' (`n` Tuple(a UInt32, b UInt32)) ENGINE = SummingMergeTree ORDER BY n SETTINGS allow_tuple_element_aggregation = 1; -- { serverError BAD_ARGUMENTS }

-- Unsupported engines accept the setting silently.
CREATE TABLE test_replacing_engine (`n` Tuple(a UInt32, b UInt32)) ENGINE = ReplacingMergeTree ORDER BY tuple() SETTINGS allow_tuple_element_aggregation = 1;
DROP TABLE test_replacing_engine;

CREATE TABLE test_collapsing_engine (`n` Tuple(a UInt32, b UInt32), `sign` Int8) ENGINE = CollapsingMergeTree(sign) ORDER BY tuple() SETTINGS allow_tuple_element_aggregation = 1;
DROP TABLE test_collapsing_engine;

CREATE TABLE test_normal_engine (`n` Tuple(a UInt32, b UInt32)) ENGINE = MergeTree ORDER BY tuple() SETTINGS allow_tuple_element_aggregation = 1;
DROP TABLE test_normal_engine;