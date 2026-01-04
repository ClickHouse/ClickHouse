-- Tags: long, zookeeper, no-replicated-database, no-shared-merge-tree
-- Tag no-replicated-database: Fails due to additional replicas or shards
-- Tag no-shared-merge-tree: no-shared-merge-tree: No quorum

DROP TABLE IF EXISTS table1;
DROP TABLE IF EXISTS table2;

CREATE TABLE table1(x UInt32) ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test_03779/table', '1') ORDER BY x;
CREATE TABLE table2(x UInt32) ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test_03779/table', '2') ORDER BY x;

set insert_quorum=2, insert_quorum_parallel=1;

set async_insert = 1;
set wait_for_async_insert = 0;
set async_insert_deduplicate = 1;
set wait_for_async_insert_timeout = 10000, async_insert_max_query_number = 1000, async_insert_max_data_size = 10000000, async_insert_use_adaptive_busy_timeout = 0;

INSERT INTO table1 VALUES (1);
INSERT INTO table1 VALUES (2);
INSERT INTO table1 VALUES (3);
SYSTEM FLUSH ASYNC INSERT QUEUE table1;

INSERT INTO table2 VALUES (2);
INSERT INTO table2 VALUES (3);
INSERT INTO table2 VALUES (4);
SYSTEM FLUSH ASYNC INSERT QUEUE table2;

SYSTEM FLUSH LOGS system.query_log;

SELECT 'q1', x FROM table1 ORDER BY all;
SELECT 'q2', x FROM table2 ORDER BY all;

SELECT
    query,
    query_kind,
    ProfileEvents['QuorumParts'] AS quorum_parts,
    ProfileEvents['QuorumWaitMicroseconds'] > 0 AS quorum_wait_microseconds_non_zero,
    ProfileEvents['QuorumFailedInserts'] AS quorum_failed_inserts
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND query_kind = 'System'
    AND has(databases, current_database())
    AND has(tables, current_database() || '.table2')
ORDER BY event_time DESC FORMAT Vertical;

DROP TABLE table1;
DROP TABLE table2;
