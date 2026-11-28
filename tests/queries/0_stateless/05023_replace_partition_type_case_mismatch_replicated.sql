-- Tags: zookeeper, no-shared-merge-tree, no-replicated-database
-- Tag no-shared-merge-tree: the test explicitly creates `ReplicatedMergeTree` tables, which the
-- `SharedMergeTree` runs do not support.
-- Tag no-replicated-database: the test creates replicas with explicit ZooKeeper paths, which must
-- not go through the replicated database machinery.

-- `REPLACE PARTITION FROM` on `ReplicatedMergeTree` goes through the same
-- `checkStructureAndGetMergeTreeData` structure comparison as plain `MergeTree`, so type-name aliases
-- and casing in key definitions must not make it report "different ordering" (see
-- `04490_replace_partition_index_type_case_mismatch` for the plain `MergeTree` matrix of such cases).

DROP TABLE IF EXISTS dst_cast_replicated SYNC;
DROP TABLE IF EXISTS src_cast_replicated SYNC;

CREATE TABLE dst_cast_replicated (x String)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/dst_cast_type', 'r1')
    ORDER BY CAST(x AS Int32) PARTITION BY tuple();
CREATE TABLE src_cast_replicated (x String)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/src_cast_type', 'r1')
    ORDER BY CAST(x AS INT) PARTITION BY tuple();

INSERT INTO src_cast_replicated VALUES ('1'), ('2');

ALTER TABLE dst_cast_replicated REPLACE PARTITION tuple() FROM src_cast_replicated;
SELECT count() FROM dst_cast_replicated;
SELECT * FROM dst_cast_replicated ORDER BY x;

DROP TABLE dst_cast_replicated SYNC;
DROP TABLE src_cast_replicated SYNC;

-- Index `TYPE` name is case-insensitive, so `SET(0)` and `set(0)` are the same index.
-- `REPLACE PARTITION FROM` must not report "different secondary indices" for them.

DROP TABLE IF EXISTS dst_idx_replicated SYNC;
DROP TABLE IF EXISTS src_idx_replicated SYNC;

CREATE TABLE dst_idx_replicated (id UInt64, INDEX idx id TYPE SET(0) GRANULARITY 4)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/dst_idx_case', 'r1')
    ORDER BY id PARTITION BY id % 2;
CREATE TABLE src_idx_replicated (id UInt64, INDEX idx id TYPE set(0) GRANULARITY 4)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/src_idx_case', 'r1')
    ORDER BY id PARTITION BY id % 2;

INSERT INTO src_idx_replicated VALUES (0), (2);

ALTER TABLE dst_idx_replicated REPLACE PARTITION 0 FROM src_idx_replicated;
SELECT count() FROM dst_idx_replicated;

DROP TABLE dst_idx_replicated SYNC;
DROP TABLE src_idx_replicated SYNC;
