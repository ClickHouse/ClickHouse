-- Tags: no-random-merge-tree-settings, no-parallel-replicas, zookeeper
-- Test `ReplicatedMergeTreeQueue` with partitioning: block numbers are
-- globally monotonic across partitions and consistent across replicas.

set enable_analyzer = 1;
set query_plan_optimize_prewhere = 1;
set optimize_move_to_prewhere = 1;
set insert_keeper_fault_injection_probability = 0;

drop table if exists rmtqp1 sync;
drop table if exists rmtqp2 sync;

CREATE TABLE rmtqp1(p UInt64, a UInt64)
ENGINE = ReplicatedMergeTreeQueue('/clickhouse/tables/{database}/rmtqp', '1')
PARTITION BY p
settings index_granularity=1;

CREATE TABLE rmtqp2(p UInt64, a UInt64)
ENGINE = ReplicatedMergeTreeQueue('/clickhouse/tables/{database}/rmtqp', '2')
PARTITION BY p
settings index_granularity=1;

-- Insert into both replicas across different partitions
insert into rmtqp1 values (1, 10) (1, 20) (1, 30);
insert into rmtqp2 values (2, 40) (2, 50) (2, 60);
insert into rmtqp1 values (1, 70) (1, 80);
insert into rmtqp2 values (2, 90) (2, 100);
insert into rmtqp1 values (2, 110) (2, 120);

system sync replica rmtqp1;
system sync replica rmtqp2;

-- Both replicas must have identical data with matching block numbers
select 'replica 1';
select p, a, _block_number, _block_offset from rmtqp1 order by p, _block_number, _block_offset;

select '';
select 'replica 2';
select p, a, _block_number, _block_offset from rmtqp2 order by p, _block_number, _block_offset;

-- Index lookup across partitions
select '';
select 'index lookup partition 1';
select p, a from rmtqp1 where (_block_number, _block_offset) = (0, 1) order by p;

select '';
select 'index lookup partition 1 explain';
explain indexes=1 select p, a from rmtqp1 where (_block_number, _block_offset) = (0, 1);

select '';
select 'index lookup partition 2';
select p, a from rmtqp1 where (_block_number, _block_offset) = (0, 1) order by p;

select '';
select 'index lookup partition 2 explain';
explain indexes=1 select p, a from rmtqp1 where (_block_number, _block_offset) = (0, 1);

-- Merge within each partition and verify
optimize table rmtqp1 final;
system sync replica rmtqp2;

select '';
select 'after merge replica 1';
select p, a, _block_number, _block_offset from rmtqp1 order by p, _block_number, _block_offset;

select '';
select 'after merge replica 2';
select p, a, _block_number, _block_offset from rmtqp2 order by p, _block_number, _block_offset;

drop table rmtqp1 sync;
drop table rmtqp2 sync;
