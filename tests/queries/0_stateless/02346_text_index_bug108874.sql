-- Tags: distributed, no-replicated-database

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/108874
-- A text index on mapValues(map)/mapKeys(map) was used for a local MergeTree table and via
-- cluster()/remote(), but silently NOT used when the same table was queried through a
-- Distributed engine table with the analyzer. The Distributed table has no secondary indices in
-- its own metadata, so FunctionToSubcolumnsPass rewrote mapValues(attributes) -> attributes.values
-- on the initiator and serialized that form to the shards, where it no longer matched the index.
-- force_data_skipping_indices makes the query throw INDEX_NOT_USED (code 277) if the index is not
-- used, so a successful query with the correct count proves the index is used through every path.

SET enable_full_text_index = 1;
SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1; -- default; the setting that triggered the bug

DROP TABLE IF EXISTS logs;
DROP TABLE IF EXISTS logs_dist;

CREATE TABLE logs
(
    attributes Map(String, String),
    INDEX attributes_vals_idx mapValues(attributes) TYPE text(tokenizer = 'array') GRANULARITY 1,
    INDEX attributes_keys_idx mapKeys(attributes) TYPE text(tokenizer = 'array') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();

CREATE TABLE logs_dist AS logs
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), logs, rand());

INSERT INTO logs VALUES ({'ip': '192.168.1.1'});

SELECT 'mapValues local',   count() FROM logs WHERE has(mapValues(attributes), '192.168.1.1')
    SETTINGS force_data_skipping_indices = 'attributes_vals_idx';
SELECT 'mapValues cluster', count() FROM cluster('test_cluster_two_shards_localhost', currentDatabase(), logs) WHERE has(mapValues(attributes), '192.168.1.1')
    SETTINGS force_data_skipping_indices = 'attributes_vals_idx';
SELECT 'mapValues dist',    count() FROM logs_dist WHERE has(mapValues(attributes), '192.168.1.1')
    SETTINGS force_data_skipping_indices = 'attributes_vals_idx';

SELECT 'mapKeys local',   count() FROM logs WHERE has(mapKeys(attributes), 'ip')
    SETTINGS force_data_skipping_indices = 'attributes_keys_idx';
SELECT 'mapKeys cluster', count() FROM cluster('test_cluster_two_shards_localhost', currentDatabase(), logs) WHERE has(mapKeys(attributes), 'ip')
    SETTINGS force_data_skipping_indices = 'attributes_keys_idx';
SELECT 'mapKeys dist',    count() FROM logs_dist WHERE has(mapKeys(attributes), 'ip')
    SETTINGS force_data_skipping_indices = 'attributes_keys_idx';

-- Wrapper storages over a Distributed table. StorageDistributed opts out of
-- supportsOptimizationToSubcolumns(), but a wrapper that ties the capability to
-- supportsSubcolumns() (the IStorage default) re-enables the mapValues -> subcolumn rewrite on
-- the initiator, so the shard query loses the index again. StorageMaterializedView must forward
-- the capability to its target table, and StorageMerge must fail closed if any child opts out.

-- Materialized view whose target is the Distributed table; query the view itself.
CREATE MATERIALIZED VIEW logs_mv TO logs_dist AS SELECT attributes FROM logs;
SELECT 'mapValues mv-over-dist', count() FROM logs_mv WHERE has(mapValues(attributes), '192.168.1.1')
    SETTINGS force_data_skipping_indices = 'attributes_vals_idx';
SELECT 'mapKeys mv-over-dist',   count() FROM logs_mv WHERE has(mapKeys(attributes), 'ip')
    SETTINGS force_data_skipping_indices = 'attributes_keys_idx';

-- Merge table over the Distributed child.
CREATE TABLE logs_merge AS logs ENGINE = Merge(currentDatabase(), '^logs_dist$');
SELECT 'mapValues merge-over-dist', count() FROM logs_merge WHERE has(mapValues(attributes), '192.168.1.1')
    SETTINGS force_data_skipping_indices = 'attributes_vals_idx';
SELECT 'mapKeys merge-over-dist',   count() FROM logs_merge WHERE has(mapKeys(attributes), 'ip')
    SETTINGS force_data_skipping_indices = 'attributes_keys_idx';

-- Buffer table over the Distributed child. getQueryProcessingStage and read() forward the
-- already-analyzed query to the destination, so like Merge/MaterializedView the Buffer must fail
-- closed instead of inheriting the IStorage default (supportsSubcolumns() == true). The buffer is
-- empty (flushed), so the SELECT reads only the destination Distributed table -> shard MergeTree,
-- and force_data_skipping_indices governs those indexed shard reads.
CREATE TABLE logs_buffer (attributes Map(String, String))
ENGINE = Buffer(currentDatabase(), logs_dist, 1, 10, 100, 10000, 1000000, 10000000, 100000000);
SELECT 'mapValues buffer-over-dist', count() FROM logs_buffer WHERE has(mapValues(attributes), '192.168.1.1')
    SETTINGS force_data_skipping_indices = 'attributes_vals_idx';
SELECT 'mapKeys buffer-over-dist',   count() FROM logs_buffer WHERE has(mapKeys(attributes), 'ip')
    SETTINGS force_data_skipping_indices = 'attributes_keys_idx';

DROP TABLE logs_buffer;
DROP TABLE logs_merge;
DROP TABLE logs_mv;
DROP TABLE logs_dist;
DROP TABLE logs;

-- Lazy-proxy variant. When a database has lazy_load_tables = 1, each table is wrapped in a
-- StorageTableProxy that forwards to the real storage on first access. StorageProxy forwarded
-- supportsSubcolumns() but not supportsOptimizationToSubcolumns(), so a proxy around Distributed
-- fell back to the IStorage default (which ties it to supportsSubcolumns() == true) and re-enabled
-- the mapValues -> subcolumn rewrite on the initiator. The index was then missed again through a
-- lazy-loaded Distributed engine table, even after StorageDistributed itself opted out.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 1;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.logs
(
    attributes Map(String, String),
    INDEX attributes_vals_idx mapValues(attributes) TYPE text(tokenizer = 'array') GRANULARITY 1,
    INDEX attributes_keys_idx mapKeys(attributes) TYPE text(tokenizer = 'array') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.logs_dist (attributes Map(String, String))
ENGINE = Distributed('test_cluster_two_shards_localhost', {CLICKHOUSE_DATABASE_1:String}, logs, rand());

INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.logs VALUES ({'ip': '192.168.1.1'});

-- Re-attach so the tables become StorageTableProxy instances.
DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

SELECT 'mapValues lazy-proxy dist', count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.logs_dist WHERE has(mapValues(attributes), '192.168.1.1')
    SETTINGS force_data_skipping_indices = 'attributes_vals_idx';
SELECT 'mapKeys lazy-proxy dist',   count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.logs_dist WHERE has(mapKeys(attributes), 'ip')
    SETTINGS force_data_skipping_indices = 'attributes_keys_idx';

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
