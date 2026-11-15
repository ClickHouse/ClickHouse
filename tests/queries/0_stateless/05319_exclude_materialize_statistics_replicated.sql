-- Tags: zookeeper, no-replicated-database, no-shared-merge-tree
-- no-replicated-database: creates explicit replicas, which the replicas of a Replicated database would duplicate
-- no-shared-merge-tree: creates two replicas of a ReplicatedMergeTree table explicitly

-- exclude_materialize_statistics_on_merge with ReplicatedMergeTree: every replica that executes the merge excludes
-- the column, and a replica that fetches the merged part gets it without statistics for the column as well.
SET allow_statistics = 1;
SET materialize_statistics_on_insert = 0;

DROP TABLE IF EXISTS tab_r1 SYNC;
DROP TABLE IF EXISTS tab_r2 SYNC;

CREATE TABLE tab_r1 (a UInt64, b UInt64, c UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05319_tab', 'r1')
ORDER BY a
SETTINGS
    enable_block_number_column = 0,
    enable_block_offset_column = 0,
    auto_statistics_types = 'basic',
    exclude_materialize_statistics_on_merge = 'b';

CREATE TABLE tab_r2 (a UInt64, b UInt64, c UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05319_tab', 'r2')
ORDER BY a
SETTINGS
    enable_block_number_column = 0,
    enable_block_offset_column = 0,
    auto_statistics_types = 'basic',
    exclude_materialize_statistics_on_merge = 'b';

SYSTEM STOP MERGES tab_r1;
SYSTEM STOP MERGES tab_r2;
INSERT INTO tab_r1 SELECT number, number, number FROM numbers(100);
INSERT INTO tab_r1 SELECT number + 100, number, number FROM numbers(100);
SYSTEM SYNC REPLICA tab_r2;
SYSTEM START MERGES tab_r1;
SYSTEM START MERGES tab_r2;

OPTIMIZE TABLE tab_r1 FINAL SETTINGS alter_sync = 2;
SYSTEM SYNC REPLICA tab_r2;

SELECT 'Both replicas have the merged part without statistics for b';
SELECT table, column, statistics != [] AS has_stats
FROM system.parts_columns
WHERE database = currentDatabase() AND table IN ('tab_r1', 'tab_r2') AND active
ORDER BY table, column;

DROP TABLE tab_r1 SYNC;
DROP TABLE tab_r2 SYNC;
