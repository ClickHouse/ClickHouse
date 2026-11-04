-- Tags: no-parallel

create database if not exists shard_0;
create database if not exists shard_1;
drop table if exists shard_0.test;
drop table if exists shard_1.test;
drop table if exists test_dist;

CREATE TABLE shard_0.test
(
    `id` UInt32,
    `name` String,
    `dtm` UInt32
)
ENGINE = MergeTree
PARTITION BY dtm
ORDER BY id
SETTINGS index_granularity = 8192;

CREATE TABLE shard_1.test
(
    `id` UInt32,
    `name` String,
    `dtm` UInt32
)
ENGINE = MergeTree
PARTITION BY dtm
ORDER BY id
SETTINGS index_granularity = 8192;

CREATE TABLE test_dist
(
    `id` UInt32,
    `name` String,
    `dtm` UInt32
)
ENGINE = Distributed('test_cluster_two_shards_different_databases', '', 'test');

insert into shard_0.test select number, number, number % 3 from numbers(6);
insert into shard_1.test select number + 3, number, (number + 1) % 3 from numbers(6);

-- { echoOn }

select _shard_num, * from test_dist order by id, _shard_num;

select count() from test_dist a where id in (select id from test_dist where dtm != 1 settings distributed_product_mode='allow') settings enable_analyzer=1;
select count() from test_dist a where id in (select id from test_dist where dtm != 1 settings distributed_product_mode='local') settings enable_analyzer=1;

select count() from test_dist a where id in (select id from test_dist where dtm != 1 settings distributed_product_mode='local') and id in (select id from test_dist where dtm != 2 settings distributed_product_mode='local') settings enable_analyzer=1;
select count() from test_dist a where id in (select id from test_dist where dtm != 1 settings distributed_product_mode='local') and id in (select id from test_dist where dtm != 2 settings distributed_product_mode='allow') settings enable_analyzer=1;
select count() from test_dist a where id in (select id from test_dist where dtm != 1 settings distributed_product_mode='allow') and id in (select id from test_dist where dtm != 2 settings distributed_product_mode='local') settings enable_analyzer=1;
select count() from test_dist a where id in (select id from test_dist where dtm != 1 settings distributed_product_mode='allow') and id in (select id from test_dist where dtm != 2 settings distributed_product_mode='allow') settings enable_analyzer=1;
