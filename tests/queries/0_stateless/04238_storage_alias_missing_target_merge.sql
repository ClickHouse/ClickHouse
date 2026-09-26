-- Tags: need-query-parameters

DROP TABLE IF EXISTS merge_over_alias_with_missing_target;
CREATE TABLE IF NOT EXISTS target_for_alias_with_missing_target (`key` UInt32) ENGINE = MergeTree ORDER BY key;
DROP TABLE IF EXISTS alias_with_missing_target;
DROP TABLE IF EXISTS target_for_alias_with_missing_target;

CREATE TABLE target_for_alias_with_missing_target (`key` UInt32) ENGINE = MergeTree ORDER BY key;
CREATE TABLE alias_with_missing_target ENGINE = Alias('target_for_alias_with_missing_target');
DROP TABLE target_for_alias_with_missing_target;

CREATE TABLE merge_over_alias_with_missing_target (`key` UInt32) ENGINE = Merge(currentDatabase(), '^alias_with_missing_target$');

SELECT * FROM merge_over_alias_with_missing_target; -- { serverError UNKNOWN_TABLE }
SELECT * FROM merge_over_alias_with_missing_target FINAL; -- { serverError UNKNOWN_TABLE }
SELECT * FROM merge_over_alias_with_missing_target SAMPLE 10; -- { serverError UNKNOWN_TABLE }

CREATE TABLE target_for_alias_with_missing_target (`key` UInt32) ENGINE = MergeTree ORDER BY key;

DROP TABLE merge_over_alias_with_missing_target;
DROP TABLE alias_with_missing_target;
DROP TABLE target_for_alias_with_missing_target;

DROP TABLE IF EXISTS merge_over_healthy_alias;
DROP TABLE IF EXISTS healthy_alias;
DROP TABLE IF EXISTS merge_over_alias_to_param_view;
DROP TABLE IF EXISTS merge_over_param_view;
DROP TABLE IF EXISTS alias_to_param_view;
DROP VIEW IF EXISTS param_view_for_alias;
DROP TABLE IF EXISTS param_view_base;

CREATE TABLE param_view_base (`a` Int32) ENGINE = MergeTree ORDER BY a;
INSERT INTO param_view_base VALUES (1), (2);
CREATE VIEW param_view_for_alias AS SELECT * FROM param_view_base WHERE a = {p:Int32};
CREATE TABLE alias_to_param_view ENGINE = Alias(currentDatabase(), param_view_for_alias);

SELECT '-- an Alias whose target is a parameterized view, read through a Merge table';
CREATE TABLE merge_over_alias_to_param_view (`a` Int32) ENGINE = Merge(currentDatabase(), '^alias_to_param_view$');
SELECT * FROM merge_over_alias_to_param_view; -- { serverError STORAGE_REQUIRES_PARAMETER }
SELECT * FROM merge_over_alias_to_param_view FINAL; -- { serverError STORAGE_REQUIRES_PARAMETER }
SELECT * FROM merge_over_alias_to_param_view SAMPLE 10; -- { serverError STORAGE_REQUIRES_PARAMETER }

SELECT '-- and through merge(), whose regexp admits the alias next to a readable table';
SELECT * FROM merge(currentDatabase(), '^(alias_to_param_view|param_view_base)$') ORDER BY a; -- { serverError STORAGE_REQUIRES_PARAMETER }

SELECT '-- the parameterized view itself keeps reporting the same error';
CREATE TABLE merge_over_param_view (`a` Int32) ENGINE = Merge(currentDatabase(), '^param_view_for_alias$');
SELECT * FROM merge_over_param_view; -- { serverError STORAGE_REQUIRES_PARAMETER }

SELECT '-- a longer Alias chain to the parameterized view reports an unsupported read';
-- The chain exists only for the read below, and the outer alias is dropped immediately after it, so
-- no point in between is reachable by a restart. A Memory database additionally keeps the pair out
-- of stored metadata.
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Memory;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.outer_alias_to_inner_alias ENGINE = Alias({CLICKHOUSE_DATABASE_1:Identifier}, inner_alias_to_param_view);
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.inner_alias_to_param_view ENGINE = Alias(currentDatabase(), param_view_for_alias);
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.merge_over_alias_chain (`a` Int32) ENGINE = Merge({CLICKHOUSE_DATABASE_1:Identifier}, '^outer_alias_to_inner_alias$');
SELECT * FROM {CLICKHOUSE_DATABASE_1:Identifier}.merge_over_alias_chain; -- { serverError UNSUPPORTED_METHOD }
DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.outer_alias_to_inner_alias;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

SELECT '-- an Alias to an ordinary table is still readable through a Merge table';
CREATE TABLE healthy_alias ENGINE = Alias(currentDatabase(), param_view_base);
CREATE TABLE merge_over_healthy_alias (`a` Int32) ENGINE = Merge(currentDatabase(), '^healthy_alias$');
SELECT * FROM merge_over_healthy_alias ORDER BY a;
SELECT * FROM merge(currentDatabase(), '^healthy_alias$') ORDER BY a;

DROP TABLE merge_over_healthy_alias;
DROP TABLE healthy_alias;
DROP TABLE merge_over_param_view;
DROP TABLE merge_over_alias_to_param_view;
DROP TABLE alias_to_param_view;
DROP VIEW param_view_for_alias;
DROP TABLE param_view_base;
