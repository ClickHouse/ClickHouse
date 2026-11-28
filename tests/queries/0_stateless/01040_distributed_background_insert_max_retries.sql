drop table if exists data;
drop table if exists dist;

create table data (key Int) engine=Null();
create table dist (key Int, value Int) engine=Distributed(test_shard_localhost, currentDatabase(), data, 1) settings background_insert_max_retries=3;

-- disable send in background to make the test behavior deterministic
system stop distributed sends dist;
set prefer_localhost_replica=0;

insert into dist values (1, 1);
-- first try will get an error
system flush distributed dist; -- { serverError NO_SUCH_COLUMN_IN_TABLE }
-- second try will get an error
system flush distributed dist; -- { serverError NO_SUCH_COLUMN_IN_TABLE }
-- third try will get an error and mark batch as broken
system flush distributed dist; -- { serverError NO_SUCH_COLUMN_IN_TABLE }
-- subsequent send will not have anything to send
system flush distributed dist;

drop table if exists ephemeral;
drop table if exists dist;

create table ephemeral (key Int, value Int) engine=MergeTree PARTITION BY key ORDER BY tuple();
create table dist (key Int, value Int) engine=Distributed(test_shard_localhost, currentDatabase(), ephemeral, rand()) settings background_insert_max_retries=3;
system stop distributed sends dist;

set prefer_localhost_replica=0;
set max_partitions_per_insert_block = 1;

insert into dist values (1,1),(2,2),(3,3);
-- first try will get an error
system flush distributed dist; -- { serverError TOO_MANY_PARTS }
select count() from dist;
-- second try will get an error
system flush distributed dist; -- { serverError TOO_MANY_PARTS }
select count() from dist;
-- second try will success
system flush distributed dist settings max_partitions_per_insert_block = 100;
select count() from dist;
-- subsequent send will not have anything to send
system flush distributed dist;
select count() from dist;
select count() from ephemeral;
