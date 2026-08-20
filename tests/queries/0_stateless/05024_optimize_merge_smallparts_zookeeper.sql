-- Tags: zookeeper
-- zookeeper: table engine is ReplicatedMergeTree.

DROP TABLE IF EXISTS t_merge_smallparts_r1 SYNC;
DROP TABLE IF EXISTS t_merge_smallparts_r2 SYNC;

CREATE TABLE t_merge_smallparts_r1 (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test/merge_smallparts', '1') ORDER BY x;
CREATE TABLE t_merge_smallparts_r2 (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test/merge_smallparts', '2') ORDER BY x;

INSERT INTO t_merge_smallparts_r1 VALUES (0);
INSERT INTO t_merge_smallparts_r1 VALUES (1);
INSERT INTO t_merge_smallparts_r1 VALUES (2);
INSERT INTO t_merge_smallparts_r1 VALUES (3);
INSERT INTO t_merge_smallparts_r1 VALUES (4);
INSERT INTO t_merge_smallparts_r1 VALUES (5);

SYSTEM SYNC REPLICA t_merge_smallparts_r2;

-- 6 single-row parts before any merge.
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_smallparts_r2' AND active;

-- LIMIT 4 must merge exactly 4 of the 6 parts, leaving 6 - 4 + 1 = 3 active parts on both replicas.
OPTIMIZE TABLE t_merge_smallparts_r1 PARTITION ID 'all' MERGE SMALLPARTS LIMIT 4 SETTINGS optimize_throw_if_noop = 1;

SYSTEM SYNC REPLICA t_merge_smallparts_r2;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_smallparts_r2' AND active;

-- No data was lost or duplicated, and it replicated correctly.
SELECT sum(x), count() FROM t_merge_smallparts_r2;

DROP TABLE t_merge_smallparts_r1 SYNC;
DROP TABLE t_merge_smallparts_r2 SYNC;
