-- Tags: no-s3-storage

drop table if exists test_agg_proj_02302;

create table test_agg_proj_02302 (x Int32, y Int32, PROJECTION x_plus_y (select sum(x - y), argMax(x, y) group by x + y)) ENGINE = MergeTree order by tuple() settings index_granularity = 1;
insert into test_agg_proj_02302 select intDiv(number, 2), -intDiv(number,3) - 1 from numbers(100);

-- { echoOn }
select x + y, sum(x - y) as s from test_agg_proj_02302 group by x + y order by s desc limit 5 settings allow_experimental_projection_optimization=1, optimize_aggregation_in_order=0, optimize_read_in_order=0;
select x + y, sum(x - y) as s from test_agg_proj_02302 group by x + y order by s desc limit 5 settings allow_experimental_projection_optimization=1, optimize_aggregation_in_order=1, optimize_read_in_order=1;

-- { echoOff }
drop table test_agg_proj_02302;
