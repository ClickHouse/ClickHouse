-- Tags: zookeeper

SET distributed_ddl_output_mode = 'none';

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier} SYNC;
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Replicated('/test/' || currentDatabase() || '/05293', 's1', 'r1');

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.src (x Int64) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW {CLICKHOUSE_DATABASE_1:Identifier}.mv ENGINE = MergeTree ORDER BY x AS SELECT x FROM {CLICKHOUSE_DATABASE_1:Identifier}.src;
CREATE OR REPLACE MATERIALIZED VIEW {CLICKHOUSE_DATABASE_1:Identifier}.mv ENGINE = MergeTree ORDER BY x AS SELECT x FROM {CLICKHOUSE_DATABASE_1:Identifier}.src;

-- Only the inner table of the current view is left in the database metadata in Keeper.
SELECT count() FROM system.zookeeper WHERE path = '/test/' || currentDatabase() || '/05293/metadata' AND name LIKE '%inner_id%';
DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.mv SYNC;
SELECT count() FROM system.zookeeper WHERE path = '/test/' || currentDatabase() || '/05293/metadata' AND name LIKE '%inner_id%';

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier} SYNC;
