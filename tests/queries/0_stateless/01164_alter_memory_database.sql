-- Tags: zookeeper, no-parallel, no-shared-merge-tree
-- no-shared-merge-tree: doesn't support databases without UUID

drop database if exists test_1164_memory;
create database test_1164_memory engine=Memory;
create table test_1164_memory.r1 (n int) engine=ReplicatedMergeTree('/test/01164/{database}/t', '1') order by n;
create table test_1164_memory.r2 (n int) engine=ReplicatedMergeTree('/test/01164/{database}/t', '2') order by n;
alter table test_1164_memory.r1 add column m int;
system sync replica test_1164_memory.r1;
system sync replica test_1164_memory.r2;
show create table test_1164_memory.r1;
show create table test_1164_memory.r1;
drop database test_1164_memory;
