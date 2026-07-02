-- Tags: zookeeper, no-shared-merge-tree
-- Test: exercises CREATE TABLE w/o columns for ReplicatedMergeTree with PARTITION BY clause —
-- this triggers the `getMinMaxCountProjection` call with non-empty partition columns and
-- non-empty primary key, validating the fix that passes `columns` (fetched from ZooKeeper)
-- instead of `args.columns` (empty) at registerStorageMergeTree.cpp:706.
-- Covers: src/Storages/MergeTree/registerStorageMergeTree.cpp:706 (extended syntax) where
-- columns from replica must be passed to projection construction.

drop table if exists rmt_part_r1 sync;
drop table if exists rmt_part_r2 sync;

-- Replica 1 with explicit columns + non-trivial PARTITION BY + ORDER BY
create table rmt_part_r1 (key Int32, val String, dt Date)
    engine=ReplicatedMergeTree('/clickhouse/test/04201_rmt_part/{database}', 'r1')
    partition by toYYYYMM(dt)
    order by key;

-- Replica 2 without explicit columns — columns are fetched from ZooKeeper.
-- This exercises the fix: minmax_count_projection must use the fetched columns,
-- not the empty `args.columns`. Without the fix, projection construction would
-- fail because partition column `dt` and order-by column `key` are referenced
-- in the projection AST but missing from the underlying storage columns.
create table rmt_part_r2
    engine=ReplicatedMergeTree('/clickhouse/test/04201_rmt_part/{database}', 'r2')
    partition by toYYYYMM(dt)
    order by key;

-- Verify schema fetched correctly
show create rmt_part_r2 format LineAsString;

-- Verify replication actually works after the fix
insert into rmt_part_r1 values (1, 'a', '2024-01-01'), (2, 'b', '2024-02-01');
system sync replica rmt_part_r2;
select count() from rmt_part_r2;
select * from rmt_part_r2 order by key;

drop table rmt_part_r1 sync;
drop table rmt_part_r2 sync;
