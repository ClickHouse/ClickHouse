drop table if exists null_01293;
drop table if exists dist_01293;

create table null_01293 (key Int) engine=Null();
create table dist_01293 as null_01293 engine=Distributed(test_cluster_two_shards, currentDatabase(), null_01293, key);

-- no rows, since no active monitor
select * from system.distribution_queue;

select 'INSERT';
system stop distributed sends dist_01293;
insert into dist_01293 select * from numbers(10);
-- metrics updated only after distributed_directory_monitor_sleep_time_ms
set distributed_directory_monitor_sleep_time_ms=10;
-- 1 second should guarantee metrics update
-- XXX: but this is kind of quirk, way more better will be account this metrics without any delays.
select sleep(1) format Null;
select is_blocked, error_count, data_files, data_compressed_bytes>100 from system.distribution_queue;
system flush distributed dist_01293;

select 'FLUSH';
select is_blocked, error_count, data_files, data_compressed_bytes from system.distribution_queue;

select 'UNBLOCK';
system start distributed sends dist_01293;
select is_blocked, error_count, data_files, data_compressed_bytes from system.distribution_queue;
