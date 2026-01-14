-- Tags: shard, no-parallel

drop table if exists local_table_1;
drop table if exists local_table_2;
drop table if exists distributed_table_1;
drop table if exists distributed_table_2;

SET prefer_localhost_replica = 0;
SET allow_experimental_analyzer = 1;
SET distributed_product_mode = 'allow';
SET prefer_global_in_and_join = 1;
SET max_rows_to_read = 100000000;
SET read_overflow_mode = 'break';

create table local_table_1 (id int) engine = MergeTree order by id;
create table local_table_2 (id int) engine = MergeTree order by id;
create table distributed_table_1 (id int) engine = Distributed(test_cluster_two_shard_three_replicas_localhost, currentDatabase(), local_table_1);
create table distributed_table_2 (id int) engine = Distributed(test_cluster_two_shard_three_replicas_localhost, currentDatabase(), local_table_2);

insert into local_table_1 select number from numbers(100);

insert into local_table_2 select 1 from numbers(1000000);
insert into local_table_2 select 2 from numbers(1000000);
insert into local_table_2 select 3 from numbers(1000000);

select id from distributed_table_1 where id in (select id from distributed_table_2) settings enable_add_distinct_to_in_subqueries = 1;
-- Query with DISTINCT optimization disabled
select id from distributed_table_1 where id in (select id from distributed_table_2) settings enable_add_distinct_to_in_subqueries = 0;

SYSTEM FLUSH LOGS query_log;


-- Compare both NetworkReceiveBytes between with_distinct and without_distinct
WITH
    -- Get the value for with_distinct
    (SELECT read_rows, ProfileEvents
     FROM system.query_log
     WHERE current_database = currentDatabase()
       AND query LIKE '%select id from distributed_table_1 where id in (select id from distributed_table_2) settings enable_add_distinct_to_in_subqueries = 1%'
       AND type = 'QueryFinish'
       AND is_initial_query
     ORDER BY event_time DESC LIMIT 1) AS q1,

    -- Get the value for without_distinct
    (SELECT read_rows, ProfileEvents
     FROM system.query_log
     WHERE current_database = currentDatabase()
       AND query LIKE '%select id from distributed_table_1 where id in (select id from distributed_table_2) settings enable_add_distinct_to_in_subqueries = 0%'
       AND type = 'QueryFinish'
       AND is_initial_query
     ORDER BY event_time DESC LIMIT 1) AS q2

SELECT
    q1.read_rows < q2.read_rows AS read_rows_optimization_effective,
    q1.ProfileEvents['NetworkSendBytes'] < q2.ProfileEvents['NetworkSendBytes'] AS send_optimization_effective,
    q1.ProfileEvents['NetworkReceiveBytes'] < q2.ProfileEvents['NetworkReceiveBytes'] AS recv_optimization_effective;


drop table if exists local_table_1;
drop table if exists local_table_2;
drop table if exists distributed_table_1;
drop table if exists distributed_table_2;
