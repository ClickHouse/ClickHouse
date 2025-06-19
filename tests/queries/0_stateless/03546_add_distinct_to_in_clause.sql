-- Tags: shard, no-parallel

drop table if exists local_table_1;
drop table if exists local_table_2;
drop table if exists distributed_table_1;
drop table if exists distributed_table_2;

SET prefer_localhost_replica=1;
SET allow_experimental_analyzer = 1;
SET distributed_product_mode = 'allow';

create table local_table_1 (id int) engine = MergeTree order by id;
create table local_table_2 (id int) engine = MergeTree order by id;
create table distributed_table_1 (id int) engine = Distributed(test_cluster_two_shard_three_replicas_localhost, currentDatabase(), local_table_1);
create table distributed_table_2 (id int) engine = Distributed(test_cluster_two_shard_three_replicas_localhost, currentDatabase(), local_table_2);

insert into local_table_1 select number from numbers(100);

insert into local_table_2 select 1 from numbers(100000);
insert into local_table_2 select 2 from numbers(100000);
insert into local_table_2 select 3 from numbers(100000);

select id from distributed_table_1 where id in (select id from distributed_table_2) settings enable_add_distinct_to_in_subqueries = 1;
-- Query with DISTINCT optimization disabled
select id from distributed_table_1 where id in (select id from distributed_table_2) settings enable_add_distinct_to_in_subqueries = 0;

EXPLAIN query tree select id from distributed_table_1 where id in (select id from distributed_table_2) settings enable_add_distinct_to_in_subqueries = 1;

drop table if exists local_table_1;
drop table if exists local_table_2;
drop table if exists distributed_table_1;
drop table if exists distributed_table_2;