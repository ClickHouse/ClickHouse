-- Tags: no-s3-storage

drop table if exists t;
create table t (n int, s String) engine MergeTree order by n;
insert into t values (1, 'a');
select count(), count(n), count(s) from cluster('test_cluster_two_shards', currentDatabase(), t);
drop table t;
