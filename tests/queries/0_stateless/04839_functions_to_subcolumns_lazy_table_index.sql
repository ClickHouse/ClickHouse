-- Tags: no-replicated-database

-- The storage snapshot of a table from a database with lazy_load_tables = 1 must carry the real
-- metadata right after ATTACH (it used to be the StorageTableProxy stub without indices, so the
-- FunctionToSubcolumnsPass rewrite silently defeated the text index).

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
