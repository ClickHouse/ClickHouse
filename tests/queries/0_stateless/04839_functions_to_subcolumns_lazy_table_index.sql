-- Tags: no-replicated-database

-- The index guard of FunctionToSubcolumnsPass must see the table's real metadata even when the
-- table lives in a database with lazy_load_tables = 1. Right after ATTACH, such a table is wrapped
-- in a StorageTableProxy whose stub metadata is seeded only with columns: no primary key and no
-- secondary indices. If the guard reads the stub, it considers no column index-protected, rewrites
-- mapValues(attributes) -> attributes.values, and the text index on mapValues(attributes) is
-- silently not used. force_data_skipping_indices turns that into INDEX_NOT_USED (code 277), so a
-- successful query with the correct count proves the index is used on first access after ATTACH.

SET enable_full_text_index = 1;
SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 1;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.logs
(
    attributes Map(String, String),
    INDEX attributes_vals_idx mapValues(attributes) TYPE text(tokenizer = 'array') GRANULARITY 1,
    INDEX attributes_keys_idx mapKeys(attributes) TYPE text(tokenizer = 'array') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.logs VALUES ({'ip': '192.168.1.1'});

-- Re-attach so the table becomes an unloaded StorageTableProxy.
DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

SELECT 'mapValues lazy direct', count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.logs WHERE has(mapValues(attributes), '192.168.1.1')
    SETTINGS force_data_skipping_indices = 'attributes_vals_idx';
SELECT 'mapKeys lazy direct',   count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.logs WHERE has(mapKeys(attributes), 'ip')
    SETTINGS force_data_skipping_indices = 'attributes_keys_idx';

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
