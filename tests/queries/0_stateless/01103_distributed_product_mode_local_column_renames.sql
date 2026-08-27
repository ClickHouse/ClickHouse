-- Tags: distributed, no-parallel

CREATE DATABASE IF NOT EXISTS test_01103;
USE test_01103;

DROP TABLE IF EXISTS t1_shard;
DROP TABLE IF EXISTS t2_shard;
DROP TABLE IF EXISTS t1_distr;
DROP TABLE IF EXISTS t2_distr;

create table t1_shard (id Int32) engine MergeTree order by id;
create table t2_shard (id Int32) engine MergeTree order by id;

create table t1_distr as t1_shard engine Distributed(test_cluster_two_shards_localhost, test_01103, t1_shard, id);
create table t2_distr as t2_shard engine Distributed(test_cluster_two_shards_localhost, test_01103, t2_shard, id);

insert into t1_shard values (42);
insert into t2_shard values (42);

SET distributed_product_mode = 'local';

select d0.id
from t1_distr d0
where d0.id in
(
    select d1.id
    from t1_distr as d1
    inner join t2_distr as d2 on d1.id = d2.id
    where d1.id  > 0
    order by d1.id
);

select t1_distr.id
from t1_distr
where t1_distr.id in
(
    select t1_distr.id
    from t1_distr as d1
    inner join t2_distr as d2 on t1_distr.id = t2_distr.id
    where t1_distr.id  > 0
    order by t1_distr.id
);

select test_01103.t1_distr.id
from test_01103.t1_distr
where test_01103.t1_distr.id in
(
    select test_01103.t1_distr.id
    from test_01103.t1_distr as d1
    inner join test_01103.t2_distr as d2 on test_01103.t1_distr.id = test_01103.t2_distr.id
    where test_01103.t1_distr.id  > 0
    order by test_01103.t1_distr.id
);

select d0.id
from t1_distr d0
join (
    select d1.id
    from t1_distr as d1
    inner join t2_distr as d2 on d1.id = d2.id
    where d1.id  > 0
    order by d1.id
) s0 using id;

select t1_distr.id
from t1_distr
join (
    select t1_distr.id
    from t1_distr as d1
    inner join t2_distr as d2 on t1_distr.id = t2_distr.id
    where t1_distr.id  > 0
    order by t1_distr.id
) s0 using id;

select test_01103.t1_distr.id
from test_01103.t1_distr
join (
    select test_01103.t1_distr.id
    from test_01103.t1_distr as d1
    inner join test_01103.t2_distr as d2 on test_01103.t1_distr.id = test_01103.t2_distr.id
    where test_01103.t1_distr.id  > 0
    order by test_01103.t1_distr.id
) s0 using id;

DROP TABLE t1_shard;
DROP TABLE t2_shard;
DROP TABLE t1_distr;
DROP TABLE t2_distr;
DROP DATABASE test_01103;
