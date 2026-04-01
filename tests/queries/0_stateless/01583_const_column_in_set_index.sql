SET query_plan_optimize_prewhere = 1;
-- Parallel replicas with local plan execute the IN subquery in both local and remote
-- contexts, doubling the rows read from numbers(10) and exceeding max_rows_to_read.
SET enable_parallel_replicas = 0;

drop table if exists insub;

create table insub (i int, j int) engine MergeTree order by i settings index_granularity = 1;
insert into insub select number a, a + 2 from numbers(10);

SET max_rows_to_read = 12; -- 10 from numbers + 2 from table
select * from insub where i in (select toInt32(3) from numbers(10));

drop table if exists insub;
