-- Tags: no-parallel
set prefer_localhost_replica = 1;

drop table if exists null_01293;
drop table if exists dist_01293;

create table null_01293 (key Int) engine=Null();
create table dist_01293 as null_01293 engine=Distributed(test_cluster_two_shards, currentDatabase(), null_01293, key);

-- no rows, since no active monitor
select * from system.distribution_queue;

select 'INSERT';
system stop distributed sends dist_01293;
insert into dist_01293 select * from numbers(10);
select is_blocked, error_count, data_files, data_compressed_bytes>100, broken_data_files, broken_data_compressed_bytes from system.distribution_queue where database = currentDatabase();
system flush distributed dist_01293;

select 'FLUSH';
select is_blocked, error_count, data_files, data_compressed_bytes, broken_data_files, broken_data_compressed_bytes from system.distribution_queue where database = currentDatabase();

select 'UNBLOCK';
system start distributed sends dist_01293;
select is_blocked, error_count, data_files, data_compressed_bytes, broken_data_files, broken_data_compressed_bytes from system.distribution_queue where database = currentDatabase();

drop table null_01293;
drop table dist_01293;
