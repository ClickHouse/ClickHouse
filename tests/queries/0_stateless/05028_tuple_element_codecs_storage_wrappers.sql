-- Tags: no-replicated-database

SET enable_tuple_element_codecs = 1;
SET allow_experimental_alias_table_engine = 1;

-- StorageAlias delegates ALTER to its target. Capability validation must use the
-- target's answer as well.
DROP TABLE IF EXISTS tuple_codec_wrapper_alias;
DROP TABLE IF EXISTS tuple_codec_wrapper_alias_target;
CREATE TABLE tuple_codec_wrapper_alias_target
(
    payload Tuple(number UInt64, text String)
)
ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE tuple_codec_wrapper_alias ENGINE = Alias(tuple_codec_wrapper_alias_target);

ALTER TABLE tuple_codec_wrapper_alias MODIFY COLUMN payload
    Tuple(number UInt64 CODEC(ZSTD(3)), text String);

DROP TABLE tuple_codec_wrapper_alias;
DROP TABLE tuple_codec_wrapper_alias_target;

-- An unloaded table in a lazy Atomic database is represented by StorageTableProxy.
-- ALTER validation must see the nested MergeTree capability.
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier}
ENGINE = Atomic SETTINGS lazy_load_tables = 1;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.tuple_codec_wrapper_lazy
(
    payload Tuple(number UInt64, text String)
)
ENGINE = MergeTree ORDER BY tuple();

DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.tuple_codec_wrapper_lazy MODIFY COLUMN payload
    Tuple(number UInt64 CODEC(ZSTD(3)), text String);

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

-- A materialized view with an external target does not own the physical streams.
-- Its local codec metadata must be rejected even when the target is MergeTree.
DROP TABLE IF EXISTS tuple_codec_wrapper_mv;
DROP TABLE IF EXISTS tuple_codec_wrapper_mv_external_merge_tree;
DROP TABLE IF EXISTS tuple_codec_wrapper_external_merge_tree;
DROP TABLE IF EXISTS tuple_codec_wrapper_source;
CREATE TABLE tuple_codec_wrapper_source
(
    payload Tuple(number UInt64, text String)
)
ENGINE = Null;

CREATE TABLE tuple_codec_wrapper_external_merge_tree
(
    payload Tuple(number UInt64, text String) CODEC(LZ4)
)
ENGINE = MergeTree ORDER BY tuple();
CREATE MATERIALIZED VIEW tuple_codec_wrapper_mv_external_merge_tree
TO tuple_codec_wrapper_external_merge_tree
(
    payload Tuple(number UInt64 CODEC(ZSTD(3)), text String)
)
AS SELECT payload FROM tuple_codec_wrapper_source; -- { serverError NOT_IMPLEMENTED }

-- An owned inner MergeTree does store the materialized view's codec metadata.
-- The outer storage is validated after that inner target has been created.

CREATE MATERIALIZED VIEW tuple_codec_wrapper_mv
(
    payload Tuple(number UInt64 CODEC(ZSTD(3)), text String)
)
ENGINE = MergeTree ORDER BY tuple()
AS SELECT payload FROM tuple_codec_wrapper_source;

DROP TABLE tuple_codec_wrapper_mv;
DROP TABLE tuple_codec_wrapper_external_merge_tree;
DROP TABLE tuple_codec_wrapper_source;
