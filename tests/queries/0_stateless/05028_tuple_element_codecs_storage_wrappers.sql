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
SELECT countSubstrings(create_table_query, 'CODEC(ZSTD(3))')
FROM system.tables
WHERE database = currentDatabase() AND name = 'tuple_codec_wrapper_alias_target';

INSERT INTO tuple_codec_wrapper_alias VALUES ((1, 'alias'));
SELECT payload FROM tuple_codec_wrapper_alias_target;

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
SELECT countSubstrings(create_table_query, 'CODEC(ZSTD(3))')
FROM system.tables
WHERE database = {CLICKHOUSE_DATABASE_1:String} AND name = 'tuple_codec_wrapper_lazy';

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

-- StorageMaterializedView owns an inner target and is validated as the outer
-- storage after that target has been created.
DROP TABLE IF EXISTS tuple_codec_wrapper_mv;
DROP TABLE IF EXISTS tuple_codec_wrapper_source;
CREATE TABLE tuple_codec_wrapper_source
(
    payload Tuple(number UInt64, text String)
)
ENGINE = Null;

CREATE MATERIALIZED VIEW tuple_codec_wrapper_mv
(
    payload Tuple(number UInt64 CODEC(ZSTD(3)), text String)
)
ENGINE = MergeTree ORDER BY tuple()
AS SELECT payload FROM tuple_codec_wrapper_source;

INSERT INTO tuple_codec_wrapper_source VALUES ((2, 'view'));
SELECT payload FROM tuple_codec_wrapper_mv;

DROP TABLE tuple_codec_wrapper_mv;
DROP TABLE tuple_codec_wrapper_source;
