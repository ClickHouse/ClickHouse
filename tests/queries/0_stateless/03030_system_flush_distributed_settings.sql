drop table if exists ephemeral;
drop table if exists dist_in;
drop table if exists data;
drop table if exists mv;
drop table if exists dist_out;

create table ephemeral (key Int, value Int) engine=Null();
create table dist_in as ephemeral engine=Distributed(test_shard_localhost, currentDatabase(), ephemeral, key) settings background_insert_batch=1;
create table data (key Int, uniq_values Int) engine=Memory();
create materialized view mv to data as select key, uniqExact(value) uniq_values from ephemeral group by key;
system stop distributed sends dist_in;
create table dist_out as data engine=Distributed(test_shard_localhost, currentDatabase(), data);

set prefer_localhost_replica=0;

insert into dist_in select number/100, number from system.numbers limit 1e6 settings max_memory_usage='20Mi';
system flush distributed dist_in; -- { serverError MEMORY_LIMIT_EXCEEDED }
system flush distributed dist_in settings max_memory_usage=0;
select count() from dist_out;
