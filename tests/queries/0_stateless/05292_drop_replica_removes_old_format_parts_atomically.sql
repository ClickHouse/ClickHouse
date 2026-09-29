-- Tags: zookeeper, no-replicated-database, no-shared-merge-tree
-- no-replicated-database: the replicas are named explicitly
-- no-shared-merge-tree: depends on the ZooKeeper layout of ReplicatedMergeTree parts

-- With the old part header format a part node has `columns` and `checksums` children. Dropping a replica
-- must remove every part node in one request with them, with the current setting 0 (r1) and 1 (r2).

DROP TABLE IF EXISTS t_r1 SYNC;
DROP TABLE IF EXISTS t_r2 SYNC;

CREATE TABLE t_r1 (k UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_old_part_header', 'r1')
    PARTITION BY k ORDER BY k SETTINGS use_minimalistic_part_header_in_zookeeper = 0;
CREATE TABLE t_r2 (k UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_old_part_header', 'r2')
    PARTITION BY k ORDER BY k SETTINGS use_minimalistic_part_header_in_zookeeper = 0;

-- No merges, so the only removals of part nodes are the two drops
SYSTEM STOP MERGES t_r1;
SYSTEM STOP MERGES t_r2;
INSERT INTO t_r1 VALUES (1), (2);
SYSTEM SYNC REPLICA t_r2;

-- The parts of r2 keep the old format, but the table now considers them flat
ALTER TABLE t_r2 MODIFY SETTING use_minimalistic_part_header_in_zookeeper = 1;

DROP TABLE t_r2 SYNC;
DROP TABLE t_r1 SYNC;

SYSTEM FLUSH LOGS zookeeper_log;

-- Per replica: the number of removed parts, and how many of them were removed in one request together with both children
SELECT replica, count(), countIf(removals = 3 AND requests = 1)
FROM
(
    SELECT
        extract(path, '/replicas/([^/]+)/parts/') AS replica,
        extract(path, '^(.*/parts/[^/]+)') AS part,
        count() AS removals,
        uniqExact(session_id, xid) AS requests
    FROM system.zookeeper_log
    WHERE event_date >= yesterday() AND type = 'Response' AND op_num = 'Remove' AND error = 'ZOK'
        AND startsWith(path, '/clickhouse/tables/' || currentDatabase() || '/t_old_part_header/replicas/')
        AND position(path, '/parts/') > 0
    GROUP BY replica, part
)
GROUP BY replica
ORDER BY replica;
