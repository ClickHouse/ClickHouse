-- Tags: no-replicated-database
-- Tag no-replicated-database: Fails due to additional replicas or shards

CREATE TABLE table1 (a Int32) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{database}/table', 'r1') ORDER BY a;
CREATE TABLE table2 (a Int32) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{database}/table', 'r2') ORDER BY a;

SYSTEM STOP FETCHES table2;
SYSTEM STOP MERGES table2;

INSERT INTO table1 SELECT 1;
INSERT INTO table1 SELECT 2;
OPTIMIZE TABLE table1;
DETACH TABLE table1;

SYSTEM START MERGES table2;
SYSTEM START FETCHES table2;

SELECT sleep(1) SETTINGS use_query_cache = 0;

SELECT last_exception like '%NO_REPLICA_HAS_PART%' FROM system.replication_queue WHERE table='table2' and database=currentDatabase() and type='MERGE_PARTS';
ATTACH TABLE table1;
