-- Tags: no-random-merge-tree-settings, no-parallel-replicas, zookeeper
-- Test `ReplicatedMergeTreeQueue`: block numbers are consistent across replicas
-- and monotonically allocated regardless of which replica receives the insert.

set enable_analyzer = 1;
set query_plan_optimize_prewhere = 1;
set optimize_move_to_prewhere = 1;
set insert_keeper_fault_injection_probability = 0;

drop table if exists rmtq1 sync;
drop table if exists rmtq2 sync;

CREATE TABLE rmtq1(a UInt64)
ENGINE = ReplicatedMergeTreeQueue('/clickhouse/tables/{database}/rmtq', '1')
settings index_granularity=1;

CREATE TABLE rmtq2(a UInt64)
ENGINE = ReplicatedMergeTreeQueue('/clickhouse/tables/{database}/rmtq', '2')
settings index_granularity=1;

-- Insert into replicas in shuffled order
insert into rmtq1 values (10) (20) (30);
insert into rmtq2 values (40) (50) (60);
insert into rmtq1 values (70) (80) (90);

system sync replica rmtq1;
system sync replica rmtq2;

-- Both replicas must have the same data with identical block numbers
select 'replica 1';
select a, _block_number, _block_offset from rmtq1 settings max_threads=1;

select '';
select 'replica 2';
select a, _block_number, _block_offset from rmtq2 settings max_threads=1;

-- Merge and verify consistency across replicas
optimize table rmtq1 final;
system sync replica rmtq2;

select '';
select 'after merge replica 1';
select a, _block_number, _block_offset from rmtq1 settings max_threads=1;

select '';
select 'after merge replica 2';
select a, _block_number, _block_offset from rmtq2 settings max_threads=1;

-- Index lookup works on both replicas
select '';
select 'index lookup replica 1';
select a from rmtq1 where (_block_number, _block_offset) = (1, 1);

select '';
select 'index lookup replica 2';
select a from rmtq2 where (_block_number, _block_offset) = (1, 1);

drop table rmtq1 sync;
drop table rmtq2 sync;
