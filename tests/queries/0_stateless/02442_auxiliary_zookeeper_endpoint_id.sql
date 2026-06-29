-- Tags: no-fasttest, no-shared-merge-tree
-- no-shared-merge-tree -- shared merge tree doesn't support aux zookeepers

drop table if exists t1_r1 sync;
drop table if exists t1_r2 sync;
drop table if exists t2 sync;

create table t1_r1 (x Int32) engine=ReplicatedMergeTree('/test/02442/{database}/t', 'r1') order by x;

create table t1_r2 (x Int32) engine=ReplicatedMergeTree('/test/02442/{database}/t', 'r2') order by x;

-- create table with same replica_path as t1_r1
create table t2 (x Int32) engine=ReplicatedMergeTree('zookeeper2:/test/02442/{database}/t', 'r1') order by x;
drop table t2 sync;

-- insert data into one replica
insert into t1_r1 select * from generateRandom('x Int32') LIMIT 10013;
system sync replica t1_r2;
select count() from t1_r2;

drop table t1_r1 sync;
drop table t1_r2 sync;
