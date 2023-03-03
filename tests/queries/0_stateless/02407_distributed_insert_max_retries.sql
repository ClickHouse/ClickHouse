drop table if exists data;
drop table if exists dist;
create table data (key Int) engine=Null();
create table dist (key Int, value Int) engine=Distributed(test_cluster_two_shards, currentDatabase(), data, 1) settings monitor_max_retries=3;
-- disable send in background to make the test behavior determine
system stop distributed sends dist;
set prefer_localhost_replica=0;
insert into dist values (1, 1);
-- first try will get an error and mark batch as broken
system flush distributed dist; -- { serverError NO_SUCH_COLUMN_IN_TABLE }
-- second try will get an error and mark batch as broken
system flush distributed dist; -- { serverError NO_SUCH_COLUMN_IN_TABLE }
-- third try will get an error and mark batch as broken
system flush distributed dist; -- { serverError NO_SUCH_COLUMN_IN_TABLE }
-- subsequent sent will not have anything to send
system flush distributed dist;