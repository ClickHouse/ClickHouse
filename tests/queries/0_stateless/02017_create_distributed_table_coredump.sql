-- Tags: distributed

drop table if exists t;
drop table if exists td1;
drop table if exists td2;
drop table if exists td3;
create table t (val UInt32) engine = MergeTree order by val;
create table td1 engine = Distributed(test_shard_localhost, currentDatabase(), 't') as t;
create table td2 engine = Distributed(test_shard_localhost, currentDatabase(), 't', xxHash32(val), default) as t;
create table td3 engine = Distributed(test_shard_localhost, currentDatabase(), 't', xxHash32(val), 'default') as t;
drop table if exists t;
drop table if exists td1; 
drop table if exists td2; 
drop table if exists td3; 
