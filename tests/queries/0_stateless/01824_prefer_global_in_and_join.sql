-- Tags: global, no-parallel

-- { echo }
CREATE DATABASE IF NOT EXISTS test_01824;
USE test_01824;

DROP TABLE IF EXISTS t1_shard;
DROP TABLE IF EXISTS t2_shard;
DROP TABLE IF EXISTS t1_distr;
DROP TABLE IF EXISTS t2_distr;

create table t1_shard (id Int32) engine MergeTree order by id;
create table t2_shard (id Int32) engine MergeTree order by id;

create table t1_distr as t1_shard engine Distributed(test_cluster_two_shards_localhost, test_01824, t1_shard, id);
create table t2_distr as t2_shard engine Distributed(test_cluster_two_shards_localhost, test_01824, t2_shard, id);

insert into t1_shard values (42);
insert into t2_shard values (42);

SET prefer_global_in_and_join = 1;

select d0.id from t1_distr d0
join (
    select d1.id
    from t1_distr as d1
    inner join t2_distr as d2 on d1.id = d2.id
    where d1.id  > 0
    order by d1.id
) s0 using id;

explain syntax select d0.id from t1_distr d0
join (
    select d1.id
    from t1_distr as d1
    inner join t2_distr as d2 on d1.id = d2.id
    where d1.id  > 0
    order by d1.id
) s0 using id;

-- Force using local mode
set distributed_product_mode = 'local';

select d0.id from t1_distr d0
join (
    select d1.id
    from t1_distr as d1
    inner join t2_distr as d2 on d1.id = d2.id
    where d1.id  > 0
    order by d1.id
) s0 using id;

explain syntax select d0.id from t1_distr d0
join (
    select d1.id
    from t1_distr as d1
    inner join t2_distr as d2 on d1.id = d2.id
    where d1.id  > 0
    order by d1.id
) s0 using id;

DROP TABLE t1_shard;
DROP TABLE t2_shard;
DROP TABLE t1_distr;
DROP TABLE t2_distr;
DROP DATABASE test_01824;
