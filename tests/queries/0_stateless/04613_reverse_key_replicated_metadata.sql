-- Tags: zookeeper, no-random-merge-tree-settings

-- A replica applies metadata changes from ZooKeeper through
-- ReplicatedMergeTreeTableMetadata::Diff::getNewMetadata, which rebuilds an explicitly defined
-- primary key from its own PRIMARY KEY clause. The clause cannot express per-column directions,
-- so the rebuilt description must inherit the DESC modifiers from the sorting key; losing them
-- would make primary key pruning analyze the reverse key as ascending and skip granules that
-- contain matching rows.

DROP TABLE IF EXISTS t_rev_replicated SYNC;
CREATE TABLE t_rev_replicated (g String, r Int8)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04613_reverse_key/t', 'r1')
ORDER BY (g, r DESC) PRIMARY KEY (g, r);
INSERT INTO t_rev_replicated VALUES ('manual', 2), ('manual', 1), ('novel', 3), ('novel', 3);

SELECT 'before alter';
SELECT count() FROM t_rev_replicated WHERE g = 'novel' AND r = 3;
SELECT count() FROM t_rev_replicated WHERE g = 'novel' AND r >= 3;

-- The ALTER is applied through the replication log, so the replica rebuilds its metadata from the
-- ZooKeeper diff; the queries must keep seeing the rows afterwards.
ALTER TABLE t_rev_replicated ADD COLUMN extra UInt8 DEFAULT 0;

SELECT 'after alter';
SELECT count() FROM t_rev_replicated WHERE g = 'novel' AND r = 3;
SELECT count() FROM t_rev_replicated WHERE g = 'novel' AND r >= 3;
SELECT count() FROM t_rev_replicated WHERE g = 'novel' AND r = 3 SETTINGS use_lightweight_primary_key_index_analysis = 0;

DETACH TABLE t_rev_replicated;
ATTACH TABLE t_rev_replicated;

SELECT 'after detach and attach';
SELECT count() FROM t_rev_replicated WHERE g = 'novel' AND r = 3;

DROP TABLE t_rev_replicated SYNC;
