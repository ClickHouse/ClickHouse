-- Tags: no-parallel, no-ordinary-database, no-replicated-database
-- Tag no-parallel: static UUID
-- Tag no-ordinary-database: requires UUID
-- Tag no-replicated-database: executes with ON CLUSTER anyway

-- Ignore "ATTACH TABLE query with full table definition is not recommended"
-- Ignore BAD_ARGUMENTS
SET send_logs_level='fatal';

DROP TABLE IF EXISTS x;

ATTACH TABLE x UUID 'aaaaaaaa-1111-2222-3333-aaaaaaaaaaaa' (key Int) ENGINE = ReplicatedMergeTree('/tables/{database}/{uuid}', 'r1') ORDER BY tuple();
SELECT uuid FROM system.tables WHERE database = currentDatabase() and table = 'x';
SELECT replica_path FROM system.replicas WHERE database = currentDatabase() and table = 'x';
DROP TABLE x;

-- {uuid} macro forbidden for CREATE TABLE without explicit UUID
CREATE TABLE x (key Int) ENGINE = ReplicatedMergeTree('/tables/{database}/{uuid}', 'r1') ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

CREATE TABLE x UUID 'aaaaaaaa-1111-2222-3333-aaaaaaaaaaaa' (key Int) ENGINE = ReplicatedMergeTree('/tables/{database}/{uuid}', 'r1') ORDER BY tuple();
SELECT uuid FROM system.tables WHERE database = currentDatabase() and table = 'x';
SELECT replica_path FROM system.replicas WHERE database = currentDatabase() and table = 'x';
DROP TABLE x;
