-- Tags: distributed

set enable_memory_bound_merging_of_aggregation_results=0;

drop table if exists projection_test;

create table projection_test (dt DateTime, cost Int64, projection p (select toStartOfMinute(dt) dt_m, sum(cost) group by dt_m)) engine MergeTree partition by toDate(dt) order by dt;

insert into projection_test with rowNumberInAllBlocks() as id select toDateTime('2020-10-24 00:00:00') + (id / 20), * from generateRandom('cost Int64', 10, 10, 1) limit 1000 settings max_threads = 1;

set optimize_use_projections = 1, force_optimize_projection = 1;
set parallel_replicas_local_plan = 1, parallel_replicas_support_projection = 1, optimize_aggregation_in_order = 0;

select toStartOfMinute(dt) dt_m, sum(cost) from projection_test group by dt_m;
select sum(cost) from projection_test;

drop table if exists projection_test_d;

create table projection_test_d (dt DateTime, cost Int64) engine Distributed(test_cluster_two_shards, currentDatabase(), projection_test);

select toStartOfMinute(dt) dt_m, sum(cost) from projection_test_d group by dt_m;
select sum(cost) from projection_test_d;

drop table projection_test;
drop table projection_test_d;
